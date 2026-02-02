/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.shard;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.*;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ShardTest {

	private static final boolean LOCAL_DEBUG = false;

	private static final N5FSTest tempN5Factory = new N5FSTest() {

		@Override public N5Writer createTempN5Writer() {

			if (LOCAL_DEBUG) {
				final N5Writer writer = new TrackingN5Writer("src/test/resources/test.n5", new FileSystemKeyValueAccess());
				writer.remove(""); // Clear old when starting new test
				return writer;
			}

			final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
			try {
				String uri = new URI("file", null, basePath, null).toString();
				return new TrackingN5Writer(uri, new FileSystemKeyValueAccess());
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			return null;
		}

		private String tempN5PathName() {

			try {
				final File tmpFile = Files.createTempDirectory("n5-shard-test-").toFile();
				tmpFile.delete();
				tmpFile.mkdir();
				tmpFile.deleteOnExit();
				return tmpFile.getCanonicalPath();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}
	};

	@Parameterized.Parameters(name = "IndexLocation({0}), Index ByteOrder({1})")
	public static Collection<Object[]> data() {

		final ArrayList<Object[]> params = new ArrayList<>();
		for (IndexLocation indexLoc : IndexLocation.values()) {
			for (ByteOrder indexByteOrder : new ByteOrder[]{ByteOrder.BIG_ENDIAN,  ByteOrder.LITTLE_ENDIAN}) {
				params.add(new Object[]{indexLoc, indexByteOrder});
			}
		}
		final int numParams = params.size();
		final Object[][] paramArray = new Object[numParams][];
		Arrays.setAll(paramArray, params::get);
		return Arrays.asList(paramArray);
	}

	@Parameterized.Parameter()
	public IndexLocation indexLocation;

	@Parameterized.Parameter(1)
	public ByteOrder indexByteOrder;

	@After
	public void removeTempWriters() {

		tempN5Factory.removeTempWriters();
	}

	private DatasetAttributes getTestAttributes(long[] dimensions, int[] shardSize, int[] blockSize) {

		return getTestAttributes(DataType.UINT8, dimensions, shardSize, blockSize);
	}

	private DatasetAttributes getTestAttributes(DataType dataType, long[] dimensions, int[] shardSize, int[] blockSize) {

		DefaultShardCodecInfo blockCodec = new DefaultShardCodecInfo(
				blockSize,
				new N5BlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				IndexLocation.END);

		return new DatasetAttributes(
				dimensions,
				shardSize,
				dataType,
				blockCodec);
	}

	protected DatasetAttributes getTestAttributes() {

		return getTestAttributes(new long[]{8, 8}, new int[]{4, 4}, new int[]{2, 2});
	}

	@Test
	public void writeReadBlocksTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();

		final DatasetAttributes datasetAttributes = getTestAttributes(
				new long[]{24, 24},
				new int[]{8, 8},
				new int[]{2, 2}
		);

		final String dataset = "writeReadBlocks";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final int numElements = blockSize[0] * blockSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		writer.writeBlocks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{11, 11}, data)
		);

		final KeyValueAccess kva = ((N5KeyValueWriter)writer).getKeyValueAccess();

		long[][] keys = new long[][]{
				{0, 0},
				{1, 0},
				{2, 2}
		};

		final long[][] someUnusedKeys = new long[][]{
				{0, 1},
				{1, 1},
				{1, 2},
				{2, 1}
		};

		ensureKeysExist(kva, writer.getURI(), dataset, datasetAttributes, keys);
		ensureKeysDoNotExist(kva, writer.getURI(), dataset, datasetAttributes, someUnusedKeys);

		final long[][] blockIndices = new long[][]{{0, 0}, {0, 1}, {1, 0}, {1, 1}, {4, 0}, {5, 0}, {11, 11}};
		for (long[] blockIndex : blockIndices) {
			final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
		}

		final byte[] data2 = new byte[numElements];
		for (int i = 0; i < data2.length; i++) {
			data2[i] = (byte)(10 + i);
		}

		writer.writeBlocks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0, 0}, data2),
				new ByteArrayDataBlock(blockSize, new long[]{1, 1}, data2),

				/* shard (0, 1) */
				new ByteArrayDataBlock(blockSize, new long[]{0, 4}, data2),
				new ByteArrayDataBlock(blockSize, new long[]{0, 5}, data2),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{10, 10}, data2));

		long[][] keys2 = new long[][]{
				{0, 0},
				{1, 0},
				{0, 1},
				{2, 2}
		};

		long[][] someUnusedKeys2 = new long[][]{
				{1, 1},
				{1, 2},
				{2, 1}
		};

		ensureKeysExist(kva, writer.getURI(), dataset, datasetAttributes, keys2);
		ensureKeysDoNotExist(kva, writer.getURI(), dataset, datasetAttributes, someUnusedKeys2);

		final long[][] oldBlockIndices = new long[][]{{0, 1}, {1, 0}, {4, 0}, {5, 0}, {11, 11}};
		for (long[] blockIndex : oldBlockIndices) {
			final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
		}

		final long[][] newBlockIndices = new long[][]{{0, 0}, {1, 1}, {0, 4}, {0, 5}, {10, 10}};
		final List<long[]> newBlockIndexList = Arrays.asList(newBlockIndices);
		final List<DataBlock<Object>> readBlocks = writer.readBlocks(dataset, datasetAttributes, newBlockIndexList);
		for (int i = 0; i < newBlockIndices.length; i++) {
			final long[] blockIndex = newBlockIndices[i];
			final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])block.getData());
			final DataBlock<?> blockFromReadBlocks = readBlocks.get(i);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])blockFromReadBlocks.getData());
		}
	}

	private void ensureKeysExist(KeyValueAccess kva, URI uri, String dataset,
			DatasetAttributes datasetAttributes, long[][] keys) {

		for (long[] key : keys) {
			final String shard = kva.compose(uri, dataset, datasetAttributes.relativeBlockPath(key));
			Assert.assertTrue("Shard at" + shard + "Does not exist", kva.exists(shard));
		}
	}

	private void ensureKeysDoNotExist(KeyValueAccess kva, URI uri, String dataset,
			DatasetAttributes datasetAttributes, long[][] keys) {

		for (long[] key : keys) {
			final String shard = kva.compose(uri, dataset, datasetAttributes.relativeBlockPath(key));
			Assert.assertFalse("Shard at" + shard + " exists but should not.", kva.exists(shard));
		}
	}

	@Test
	public void writeShardDataSizeTest() {

		// note: this test depends on the use of raw compression
		final N5Writer writer = tempN5Factory.createTempN5Writer();

		int numBlocksPerShard = 16;
		final int n5HeaderSizeBytes = 12; // 2 + 2 + 4*2
		final DatasetAttributes attrs = getTestAttributes(
				new long[]{24, 24},
				new int[]{8, 8},
				new int[]{2, 2}
		);

		final String dataset = "writeBlocksShardSize";
		writer.remove(dataset);
		final DatasetAttributes datasetAttributes = writer.createDataset(dataset, attrs);
		assertTrue(datasetAttributes.isSharded());

		final KeyValueAccess kva = ((N5KeyValueWriter)writer).getKeyValueAccess();

		final int[] blockSize = datasetAttributes.getBlockSize();
		final int numElements = blockSize[0] * blockSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		/*
		 * No blocks or shards exist.
		 * Calling readBlocks should return a list that is the same length as the requested grid positions,
		 * and every entry should be null.
		 */
		final long[][] newBlockIndices = new long[][]{{0, 0}, {1, 1}, {0, 4}, {0, 5}, {10, 10}};
		final List<DataBlock<Object>> readBlocks = writer.readBlocks(dataset, datasetAttributes, Arrays.asList(newBlockIndices));
		assertEquals(newBlockIndices.length, readBlocks.size());
		assertTrue("readBlocks for empty shard: all blocks null", readBlocks.stream().allMatch(blk -> blk == null));

		/*
		 * Now write blocks
		 */
		writer.writeBlocks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{11, 11}, data)
		);

		final int indexSizeBytes = numBlocksPerShard * 16; // 8 bytes per long *
		final int blockDataSizeBytes = numElements + n5HeaderSizeBytes;

		// shard 0,0 has 4 blocks so should be this size:
		long shard00SizeBytes = indexSizeBytes + 4 * blockDataSizeBytes;
		long shard10SizeBytes = indexSizeBytes + 2 * blockDataSizeBytes;
		long shard22SizeBytes = indexSizeBytes + 1 * blockDataSizeBytes;

		final String[][] keys = new String[][]{
				{dataset, "0", "0"},
				{dataset, "1", "0"},
				{dataset, "2", "2"}
		};

		long[] shardSizes = new long[]{
				shard00SizeBytes,
				shard10SizeBytes,
				shard22SizeBytes
		};

		int i = 0;
		for (String[] key : keys) {
			final String shardPath = kva.compose(writer.getURI(), key);
			Assert.assertEquals("shard at " + shardPath + " was the wrong size", shardSizes[i++], kva.size(shardPath));
		}

	}

	@Test
	public void writeReadBlockTest() {

		final GsonKeyValueN5Writer writer = (GsonKeyValueN5Writer)tempN5Factory.createTempN5Writer();
		final DatasetAttributes datasetAttributes = getTestAttributes();

		final String dataset = "writeReadBlock";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenBlocks = new HashMap<>();

		for (int idx1 = 1; idx1 >= 0; idx1--) {
			for (int idx2 = 1; idx2 >= 0; idx2--) {
				final long[] gridPosition = {idx1, idx2};
				final DataBlock<byte[]> dataBlock = (DataBlock<byte[]>)dataType.createDataBlock(blockSize, gridPosition, numElements);
				byte[] data = dataBlock.getData();
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
				}
				writer.writeBlock(dataset, datasetAttributes, dataBlock);

				final DataBlock<byte[]> block = writer.readBlock(dataset, datasetAttributes, dataBlock.getGridPosition().clone());
				Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());

				for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
					final long[] otherGridPosition = entry.getKey();
					final byte[] otherData = entry.getValue();
					final DataBlock<?> otherBlock = writer.readBlock(dataset, datasetAttributes, otherGridPosition);
					Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
				}

				writtenBlocks.put(gridPosition, data);
			}
		}
	}

	@Test
	public void writeReadShardTest() {

		try ( final N5Writer n5 = tempN5Factory.createTempN5Writer() ) {

			final int[] shardSize = new int[] {4,4};
			final int shardN = 16;

			final int[] blockSize = new int[] {2,2};
			final int blockN = 4;

			final String dataset = "writeReadShard";
			DatasetAttributes attrs = getTestAttributes(DataType.INT32, new long[]{8, 8}, shardSize, blockSize);

			final int[] shardData = range(shardN);
			IntArrayDataBlock shard = new IntArrayDataBlock(shardSize, new long[]{0, 0}, shardData);

			n5.writeShard(dataset, attrs, shard);
			DataBlock<int[]> readShard = n5.readShard(dataset, attrs, 0, 0);
			assertArrayEquals(shardData, readShard.getData());


			/**
			 * The 4x4 shard at (0,0)
			 * and the 2x2 blocks it contains
			 *
			 *
			 * 	0   1  |  2   3
			 *  4   5  |  6	  7
			 *  ----------------
			 *  8	9  | 10	 11
			 * 12  13  | 14  15
			 */

			assertArrayEquals(new int[]{0, 1, 4, 5}, (int[])n5.readBlock(dataset, attrs, 0, 0).getData());
			assertArrayEquals(new int[]{2, 3, 6, 7}, (int[])n5.readBlock(dataset, attrs, 1, 0).getData());
			assertArrayEquals(new int[]{8, 9, 12, 13}, (int[])n5.readBlock(dataset, attrs, 0, 1).getData());
			assertArrayEquals(new int[]{10, 11, 14, 15}, (int[])n5.readBlock(dataset, attrs, 1, 1).getData());

			n5.deleteBlock(dataset, attrs, new long[]{1, 1});

			/**
			 * After deleting block (1,1)
			 *
			 * 	0   1  |  2   3
			 *  4   5  |  6	  7
			 *  ----------------
			 *  8	9  |  0	  0
			 * 12  13  |  0   0
			 */
			final DataBlock<int[]> partlyEmptyShard = n5.readShard(dataset, attrs, 0, 0);
			assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 12, 13, 0, 0}, partlyEmptyShard.getData());


			// Delete the rest of the blocks
			n5.deleteBlocks(dataset, attrs,
					Stream.of( new long[] {0,0}, new long[] {1,0}, new long[] {0,1}).collect(Collectors.toList()));

			assertNull(n5.readShard(dataset, attrs, 0, 0));
		}
	}

	/**
	 * Checks how many read calls to the backend are performed for a particular readBlocks
	 * call. At this time (Nov 4 2025), one read for the index, and one read per block are performed.
	 */
	public void numReadsTest() {

		final TrackingN5Writer writer = (TrackingN5Writer)tempN5Factory.createTempN5Writer();

		final DatasetAttributes datasetAttributes = getTestAttributes(
				new long[]{24, 24},
				new int[]{8, 8},
				new int[]{2, 2}
		);

		final String dataset = "writeReadBlocks";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final int numElements = blockSize[0] * blockSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		writer.writeBlocks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{11, 11}, data)
		);

		writer.resetNumMaterializeCalls();
		writer.readBlocks(dataset, datasetAttributes, Collections.singletonList(new long[] {0,0}));
		System.out.println(writer.getNumMaterializeCalls());

		ArrayList<long[]> ptList = new ArrayList<>();
		ptList.add(new long[] {0, 0});
		ptList.add(new long[] {0, 1});
		ptList.add(new long[] {1, 0});
		ptList.add(new long[] {1, 1});

		writer.resetNumMaterializeCalls();
		writer.readBlocks(dataset, datasetAttributes, ptList);
		System.out.println(writer.getNumMaterializeCalls());
		System.out.println("");
	}

    @Test
    public void shardExistsTest() {

        final N5Writer writer = tempN5Factory.createTempN5Writer();

        final DatasetAttributes datasetAttributes = getTestAttributes(
                new long[]{24, 24},
                new int[]{8, 8},
                new int[]{2, 2}
        );

        final String dataset = "shardExists";
        writer.remove(dataset);
        DatasetAttributes attrs = writer.createDataset(dataset, datasetAttributes);

        final int[] blockSize = datasetAttributes.getBlockSize();
        final int numElements = blockSize[0] * blockSize[1];

        final byte[] data = new byte[numElements];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)(i);
        }

        /* write blocks to shards (0,0), (1,0), and (2,2) */
        writer.writeBlocks(
                dataset,
                attrs,
                new ByteArrayDataBlock(blockSize, new long[]{0, 0}, data),  /* shard (0, 0) */
                new ByteArrayDataBlock(blockSize, new long[]{4, 0}, data),  /* shard (1, 0) */
                new ByteArrayDataBlock(blockSize, new long[]{11, 11}, data) /* shard (2, 2) */
        );

        TrackingN5Writer trackingWriter = ((TrackingN5Writer) writer);

        Predicate<long[]> assertShardExistsTracking = (gridPosition) -> {
            trackingWriter.resetAllTracking();
            final boolean exists = writer.shardExists(dataset, attrs, gridPosition);
            assertEquals("isFileCheck incremented", 1, trackingWriter.getNumIsFileCalls());
            assertEquals("No Bytes Read", 0, trackingWriter.getTotalBytesRead());
            return exists;
        };


        trackingWriter.resetAllTracking();
        /* shards that should exist should only check file  */
        Assert.assertTrue("Shard (0,0) should exist", assertShardExistsTracking.test(new long[]{0, 0}));
        Assert.assertTrue("Shard (1,0) should exist", assertShardExistsTracking.test(new long[]{1, 0}));
        Assert.assertTrue("Shard (2,2) should exist", assertShardExistsTracking.test(new long[]{2, 2}));

        /* shards that should NOT exist */
        Assert.assertFalse("Shard (0,1) should not exist", assertShardExistsTracking.test(new long[]{0, 1}));
        Assert.assertFalse("Shard (1,1) should not exist", assertShardExistsTracking.test(new long[]{1, 1}));
        Assert.assertFalse("Shard (2,0) should not exist", assertShardExistsTracking.test(new long[]{2, 0}));
        Assert.assertFalse("Shard (0,2) should not exist", assertShardExistsTracking.test(new long[]{0, 2}));
    }

	/**
	 * Checks how many read calls to the backend are performed for a particular readBlocks
	 * call. At this time (Jan 4 2026), one read for the index, and one read per block are performed.
	 */
	@Test
	public void testPartialReadAggregationBehavior() {

        final DatasetAttributes datasetAttributes = getTestAttributes(
                new long[]{24, 24},
                new int[]{8, 8},
                new int[]{2, 2}
        );

		try (TrackingN5Writer writer = (TrackingN5Writer)tempN5Factory.createTempN5Writer()) {

	        final String dataset = "shardExists";
	        writer.remove(dataset);
	        DatasetAttributes attrs = writer.createDataset(dataset, datasetAttributes);

	        final int[] blockSize = attrs.getBlockSize();
	        final int numElements = blockSize[0] * blockSize[1];

	        final byte[] data = new byte[numElements];
	        for (int i = 0; i < data.length; i++) {
	            data[i] = (byte)(i);
	        }

			// four blocks in shard (0,0)
			ArrayList<long[]> ptList = new ArrayList<>();
			ptList.add(new long[] {0,0});
			ptList.add(new long[] {0,1});
			ptList.add(new long[] {1,0});
			ptList.add(new long[] {1,1});

	        /* write blocks to shard (0,0) */
			writer.writeBlocks(
					dataset,
					datasetAttributes,
					new ByteArrayDataBlock(blockSize, ptList.get(0), data),
					new ByteArrayDataBlock(blockSize, ptList.get(1), data),
					new ByteArrayDataBlock(blockSize, ptList.get(2), data),
					new ByteArrayDataBlock(blockSize, ptList.get(3), data)
			);

			writer.resetNumMaterializeCalls();
			writer.readBlocks(dataset, datasetAttributes, ptList);

			// TODO change this if and when we implement aggregation of read calls
			// one for the index, one for each of the four blocks
			assertEquals(5, writer.getNumMaterializeCalls());

			writer.resetNumMaterializeCalls();
			writer.readShard(dataset, datasetAttributes, new long[] {0,0});
			// one for the index, one for each of the four blocks
			assertEquals(5, writer.getNumMaterializeCalls());
		}
	}

	private int[] range(int N) {
		return IntStream.range(0, N).toArray();
	}

}