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
import java.util.Objects;
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
import static org.junit.Assert.assertFalse;
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

	private DatasetAttributes getTestAttributes(long[] dimensions, int[] shardSize, int[] chunkSize) {

		return getTestAttributes(DataType.UINT8, dimensions, shardSize, chunkSize);
	}

	private DatasetAttributes getTestAttributes(DataType dataType, long[] dimensions, int[] shardSize, int[] chunkSize) {

		DefaultShardCodecInfo blockCodec = new DefaultShardCodecInfo(
				chunkSize,
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
	public void writeReadChunksTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();

		final DatasetAttributes datasetAttributes = getTestAttributes(
				new long[]{24, 24},
				new int[]{8, 8},
				new int[]{2, 2}
		);

		final String dataset = "writeReadBlocks";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);

		final int[] chunkSize = datasetAttributes.getBlockSize();
		final int numElements = chunkSize[0] * chunkSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		writer.writeChunks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(chunkSize, new long[]{11, 11}, data)
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

		final long[][] chunkIndices = new long[][]{{0, 0}, {0, 1}, {1, 0}, {1, 1}, {4, 0}, {5, 0}, {11, 11}};
		for (long[] chunkIndex : chunkIndices) {
			final DataBlock<?> chunk = writer.readChunk(dataset, datasetAttributes, chunkIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])chunk.getData());
		}

		final byte[] data2 = new byte[numElements];
		for (int i = 0; i < data2.length; i++) {
			data2[i] = (byte)(10 + i);
		}

		writer.writeChunks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{0, 0}, data2),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 1}, data2),

				/* shard (0, 1) */
				new ByteArrayDataBlock(chunkSize, new long[]{0, 4}, data2),
				new ByteArrayDataBlock(chunkSize, new long[]{0, 5}, data2),

				/* shard (2, 2) */
				new ByteArrayDataBlock(chunkSize, new long[]{10, 10}, data2));

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

		final long[][] oldChunkIndices = new long[][]{{0, 1}, {1, 0}, {4, 0}, {5, 0}, {11, 11}};
		for (long[] chunkIndex : oldChunkIndices) {
			final DataBlock<?> chunk = writer.readChunk(dataset, datasetAttributes, chunkIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])chunk.getData());
		}

		final long[][] newChunkIndices = new long[][]{{0, 0}, {1, 1}, {0, 4}, {0, 5}, {10, 10}};
		final List<long[]> newChunkIndexList = Arrays.asList(newChunkIndices);
		final List<DataBlock<Object>> readChunks = writer.readChunks(dataset, datasetAttributes, newChunkIndexList);
		for (int i = 0; i < newChunkIndices.length; i++) {
			final long[] chunkIndex = newChunkIndices[i];
			final DataBlock<?> chunk = writer.readChunk(dataset, datasetAttributes, chunkIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])chunk.getData());
			final DataBlock<?> chunkFromReadChunks = readChunks.get(i);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])chunkFromReadChunks.getData());
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

		int numChunksPerShard = 16;
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

		final int[] chunkSize = datasetAttributes.getChunkSize();
		final int numElements = chunkSize[0] * chunkSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		/*
		 * No chunks or shards exist.
		 * Calling readChunks should return a list that is the same length as the requested grid positions,
		 * and every entry should be null.
		 */
		final long[][] newChunkIndices = new long[][]{{0, 0}, {1, 1}, {0, 4}, {0, 5}, {10, 10}};
		final List<DataBlock<Object>> readChunks = writer.readChunks(dataset, datasetAttributes, Arrays.asList(newChunkIndices));
		assertEquals(newChunkIndices.length, readChunks.size());
		assertTrue("readChunks for empty shard: all chunks null", readChunks.stream().allMatch(Objects::isNull));

		/*
		 * Now write chunks
		 */
		writer.writeChunks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(chunkSize, new long[]{11, 11}, data)
		);

		final int indexSizeBytes = numChunksPerShard * 16; // 8 bytes per long *
		final int chunkDataSizeBytes = numElements + n5HeaderSizeBytes;

		// shard 0,0 has 4 chunks so should be this size:
		long shard00SizeBytes = indexSizeBytes + 4 * chunkDataSizeBytes;
		long shard10SizeBytes = indexSizeBytes + 2 * chunkDataSizeBytes;
		long shard22SizeBytes = indexSizeBytes + 1 * chunkDataSizeBytes;

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
	public void writeReadChunkTest() {

		final GsonKeyValueN5Writer writer = (GsonKeyValueN5Writer)tempN5Factory.createTempN5Writer();
		final DatasetAttributes datasetAttributes = getTestAttributes();

		final String dataset = "writeReadBlock";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);

		final int[] chunkSize = datasetAttributes.getChunkSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenChunks = new HashMap<>();

		for (int idx1 = 1; idx1 >= 0; idx1--) {
			for (int idx2 = 1; idx2 >= 0; idx2--) {
				final long[] gridPosition = {idx1, idx2};
				final DataBlock<byte[]> chunk = (DataBlock<byte[]>)dataType.createDataBlock(chunkSize, gridPosition, numElements);
				byte[] data = chunk.getData();
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
				}
				writer.writeChunk(dataset, datasetAttributes, chunk);

				final DataBlock<byte[]> readChunk = writer.readChunk(dataset, datasetAttributes, chunk.getGridPosition().clone());
				Assert.assertArrayEquals("Read from shard doesn't match", data, readChunk.getData());

				for (Map.Entry<long[], byte[]> entry : writtenChunks.entrySet()) {
					final long[] otherGridPosition = entry.getKey();
					final byte[] otherData = entry.getValue();
					final DataBlock<byte[]> otherChunk = writer.readChunk(dataset, datasetAttributes, otherGridPosition);
					Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, otherChunk.getData());
				}

				writtenChunks.put(gridPosition, data);
			}
		}
	}

	@Test
	public void writeReadShardTest() {

		try ( final N5Writer n5 = tempN5Factory.createTempN5Writer() ) {

			final int[] shardSize = new int[] {4,4};
			final int shardN = 16;

			final int[] chunkSize = new int[] {2,2};

			final String dataset = "writeReadShard";
			DatasetAttributes attrs = getTestAttributes(DataType.INT32, new long[]{8, 8}, shardSize, chunkSize);

			final int[] shardData = range(shardN);
			IntArrayDataBlock shard = new IntArrayDataBlock(shardSize, new long[]{0, 0}, shardData);

			n5.writeBlock(dataset, attrs, shard);
			DataBlock<int[]> readBlock = n5.readBlock(dataset, attrs, 0, 0);
			assertArrayEquals(shardData, readBlock.getData());


			/**
			 * The 4x4 shard at (0,0)
			 * and the 2x2 chunks it contains
			 *
			 *
			 * 	0   1  |  2   3
			 *  4   5  |  6	  7
			 *  ----------------
			 *  8	9  | 10	 11
			 * 12  13  | 14  15
			 */

			assertArrayEquals(new int[]{0, 1, 4, 5}, (int[])n5.readChunk(dataset, attrs, 0, 0).getData());
			assertArrayEquals(new int[]{2, 3, 6, 7}, (int[])n5.readChunk(dataset, attrs, 1, 0).getData());
			assertArrayEquals(new int[]{8, 9, 12, 13}, (int[])n5.readChunk(dataset, attrs, 0, 1).getData());
			assertArrayEquals(new int[]{10, 11, 14, 15}, (int[])n5.readChunk(dataset, attrs, 1, 1).getData());

			n5.deleteChunk(dataset, attrs, new long[]{1, 1});

			/**
			 * After deleting chunk (1,1)
			 *
			 * 	0   1  |  2   3
			 *  4   5  |  6	  7
			 *  ----------------
			 *  8	9  |  0	  0
			 * 12  13  |  0   0
			 */
			final DataBlock<int[]> partlyEmptyShard = n5.readBlock(dataset, attrs, 0, 0);
			assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 12, 13, 0, 0}, partlyEmptyShard.getData());


			// Delete the rest of the chunks
			n5.deleteChunks(dataset, attrs,
					Stream.of( new long[] {0,0}, new long[] {1,0}, new long[] {0,1}).collect(Collectors.toList()));

			assertNull(n5.readBlock(dataset, attrs, 0, 0));


			// write the shard again
			n5.writeBlock(dataset, attrs, shard);

			// delete the chard
			// ensure it returns true because the shard exists
			assertTrue(n5.deleteBlock(dataset, attrs, shard.getGridPosition()));

			// ensure it returns false when the shard does not exist
			assertFalse(n5.deleteBlock(dataset, attrs, shard.getGridPosition()));

			// readBlock must return null for the deleted shard
			assertNull(n5.readBlock(dataset, attrs, shard.getGridPosition()));
		}
	}

	/**
	 * Checks how many read calls to the backend are performed for a particular readChunks
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

		final int[] chunkSize = datasetAttributes.getChunkSize();
		final int numElements = chunkSize[0] * chunkSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}

		writer.writeChunks(
				dataset,
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{0, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{0, 1}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{1, 1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(chunkSize, new long[]{4, 0}, data),
				new ByteArrayDataBlock(chunkSize, new long[]{5, 0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(chunkSize, new long[]{11, 11}, data)
		);

        writer.resetNumMaterializeCalls();
        writer.readChunks(dataset, datasetAttributes, Collections.singletonList(new long[] {0,0}));

		ArrayList<long[]> ptList = new ArrayList<>();
		ptList.add(new long[] {0, 0});
		ptList.add(new long[] {0, 1});
		ptList.add(new long[] {1, 0});
		ptList.add(new long[] {1, 1});

        writer.resetNumMaterializeCalls();
        writer.readChunks(dataset, datasetAttributes, ptList);
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

        final int[] chunkSize = datasetAttributes.getChunkSize();
        final int numElements = chunkSize[0] * chunkSize[1];

        final byte[] data = new byte[numElements];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)(i);
        }

        /* write blocks to shards (0,0), (1,0), and (2,2) */
        writer.writeChunks(
                dataset,
                attrs,
                new ByteArrayDataBlock(chunkSize, new long[]{0, 0}, data),  /* shard (0, 0) */
                new ByteArrayDataBlock(chunkSize, new long[]{4, 0}, data),  /* shard (1, 0) */
                new ByteArrayDataBlock(chunkSize, new long[]{11, 11}, data) /* shard (2, 2) */
        );

        TrackingN5Writer trackingWriter = ((TrackingN5Writer) writer);

        Predicate<long[]> assertShardExistsTracking = (gridPosition) -> {
            trackingWriter.resetAllTracking();
            final boolean exists = writer.blockExists(dataset, attrs, gridPosition);
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

	        final int[] chunkSize = attrs.getChunkSize();
	        final int numElements = chunkSize[0] * chunkSize[1];

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
			writer.writeChunks(
					dataset,
					datasetAttributes,
					new ByteArrayDataBlock(chunkSize, ptList.get(0), data),
					new ByteArrayDataBlock(chunkSize, ptList.get(1), data),
					new ByteArrayDataBlock(chunkSize, ptList.get(2), data),
					new ByteArrayDataBlock(chunkSize, ptList.get(3), data)
			);

			writer.resetNumMaterializeCalls();
			writer.readChunks(dataset, datasetAttributes, ptList);

			// one for the index, one for the four blocks (aggregated)
			assertEquals(2, writer.getNumMaterializeCalls());

			writer.resetNumMaterializeCalls();
			writer.readBlock(dataset, datasetAttributes, new long[] {0,0});
			// one for the index, one for the four blocks (aggregated)
			assertEquals(2, writer.getNumMaterializeCalls());


			/**
			 *  Aggregate read calls
			 */
            writer.tkva.aggregate = true;
			writer.resetNumMaterializeCalls();
			writer.readChunks(dataset, datasetAttributes, ptList);

			// one for the index, one that covers ALL the blocks)
			assertEquals(2, writer.getNumMaterializeCalls());

			writer.resetNumMaterializeCalls();
			writer.readBlock(dataset, datasetAttributes, new long[] {0,0});
			// one for the index, one that covers ALL the blocks
			assertEquals(2, writer.getNumMaterializeCalls());
		}
	}

	private int[] range(int N) {
		return IntStream.range(0, N).toArray();
	}

}