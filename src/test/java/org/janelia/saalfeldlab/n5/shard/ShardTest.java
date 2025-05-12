package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

@RunWith(Parameterized.class)
public class ShardTest {

	private static final boolean LOCAL_DEBUG = true;

	private static final N5FSTest tempN5Factory = new N5FSTest() {

		@Override public N5Writer createTempN5Writer() {

			if (LOCAL_DEBUG) {
				final N5Writer writer = N5Factory.createWriter("src/test/resources/test.n5");
				writer.remove(""); //Clear old when starting new test
				return writer;
			}
			return super.createTempN5Writer();
		}
	};

	@Parameterized.Parameters(name = "IndexLocation({0}), Block ByteOrder({1}), Index ByteOrder({2})")
	public static Collection<Object[]> data() {

		final ArrayList<Object[]> params = new ArrayList<>();
		for (IndexLocation indexLoc : IndexLocation.values()) {
			for (ByteOrder blockByteOrder : new ByteOrder[]{ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN}) {
				for (ByteOrder indexByteOrder : new ByteOrder[]{ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN}) {
					params.add(new Object[]{indexLoc, blockByteOrder, indexByteOrder});
				}
			}
		}
		final int numParams = LOCAL_DEBUG ? 1 : params.size();
		final Object[][] paramArray = new Object[numParams][];
		Arrays.setAll(paramArray, params::get);
		return Arrays.asList(paramArray);
	}

	@Parameterized.Parameter()
	public IndexLocation indexLocation;

	@Parameterized.Parameter(1)
	public ByteOrder dataByteOrder;

	@Parameterized.Parameter(2)
	public ByteOrder indexByteOrder;

	@After
	public void removeTempWriters() {

		tempN5Factory.removeTempWriters();
	}

	private DatasetAttributes getTestAttributes(long[] dimensions, int[] shardSize, int[] blockSize) {

		return new DatasetAttributes(
				dimensions,
				shardSize,
				blockSize,
				DataType.UINT8,
				new ShardingCodec<>(
						blockSize,
						new Codec[]{new N5BlockCodec<>()}, //, new GzipCompression(4)},
						new DeterministicSizeCodec[]{new N5BlockCodec<>(), new Crc32cChecksumCodec()},
						indexLocation
				)
		);
	}

	private DatasetAttributes getTestAttributes() {

		return getTestAttributes(new long[]{8, 8}, new int[]{4, 4}, new int[]{2, 2});
	}

	@Test
	public void writeReadBlocksTest() throws IOException {

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

		final String[][] keys = new String[][]{
				{dataset, "0", "0"},
				{dataset, "1", "0"},
				{dataset, "2", "2"}
		};
		for (String[] key : keys) {
			final String shard = kva.compose(writer.getURI(), key);
			Assert.assertTrue("Shard at" + Arrays.toString(key) + "Does not exist", kva.exists(shard));
		}

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
				new ByteArrayDataBlock(blockSize, new long[]{10, 10}, data2)
		);

		final String[][] keys2 = new String[][]{
				{dataset, "0", "0"},
				{dataset, "1", "0"},
				{dataset, "0", "1"},
				{dataset, "2", "2"}
		};
		for (String[] key : keys2) {
			final String shard = kva.compose(writer.getURI(), key);
			Assert.assertTrue("Shard at" + Arrays.toString(key) + "Does not exist", kva.exists(shard));
		}

		final long[][] oldBlockIndices = new long[][]{{0, 1}, {1, 0}, {4, 0}, {5, 0}, {11, 11}};
		for (long[] blockIndex : oldBlockIndices) {
			final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
		}

		final long[][] newBlockIndices = new long[][]{{0, 0}, {1, 1}, {0, 4}, {0, 5}, {10, 10}};
		for (long[] blockIndex : newBlockIndices) {
			final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])block.getData());
		}
	}

	@Test
	public void writeReadBlockTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();
		final DatasetAttributes datasetAttributes = getTestAttributes();

		final String dataset = "writeReadBlock";
		writer.remove(dataset);
		writer.createDataset(dataset, datasetAttributes);
		writer.deleteBlock(dataset, 0, 0); //TODO Caleb: We are abusing this here. It shouldn't delete the entire shard..

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenBlocks = new HashMap<>();

//		for (int idx1 = 1; idx1 >= 0; idx1--) {
//			for (int idx2 = 1; idx2 >= 0; idx2--) {
//				final long[] gridPosition = {idx1, idx2};
//				final DataBlock<?> dataBlock = dataType.createDataBlock(blockSize, gridPosition, numElements);
//				byte[] data = (byte[])dataBlock.getData();
//				for (int i = 0; i < data.length; i++) {
//					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
//				}
//				writer.writeBlock(dataset, datasetAttributes, dataBlock);
//
//				final DataBlock<?> block = writer.readBlock(dataset, datasetAttributes, dataBlock.getGridPosition().clone());
//				Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
//
//				for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
//					final long[] otherGridPosition = entry.getKey();
//					final byte[] otherData = entry.getValue();
//					final DataBlock<?> otherBlock = writer.readBlock(dataset, datasetAttributes, otherGridPosition);
//					Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
//				}
//
//				writtenBlocks.put(gridPosition, data);
//			}
//		}
	}

	@Test
	public void writeReadShardTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();

		final DatasetAttributes datasetAttributes = getTestAttributes();

		final String dataset = "writeReadShard";
		writer.createDataset(dataset, datasetAttributes);
		writer.deleteBlock(dataset, 0, 0);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenBlocks = new HashMap<>();

		final InMemoryShard<byte[]> shard = new InMemoryShard<>(datasetAttributes, new long[]{0, 0});

//		for (int idx1 = 1; idx1 >= 0; idx1--) {
//			for (int idx2 = 1; idx2 >= 0; idx2--) {
//				final long[] gridPosition = {idx1, idx2};
//				final DataBlock<?> dataBlock = dataType.createDataBlock(blockSize, gridPosition, numElements);
//				byte[] data = (byte[])dataBlock.getData();
//				for (int i = 0; i < data.length; i++) {
//					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
//				}
//				shard.addBlock((DataBlock<byte[]>)dataBlock);
//				writtenBlocks.put(gridPosition, data);
//			}
//		}

		writer.writeShard(dataset, datasetAttributes, shard);

		for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
			final long[] otherGridPosition = entry.getKey();
			final byte[] otherData = entry.getValue();
			final DataBlock<?> otherBlock = writer.readBlock(dataset, datasetAttributes, otherGridPosition);
			Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
		}
	}

	@Test
	@Ignore
	public void writeReadNestedShards() {

		int[] blockSize = new int[]{4, 4};
		int N = Arrays.stream(blockSize).reduce(1, (x, y) -> x * y);

		final N5Writer writer = tempN5Factory.createTempN5Writer();
		final DatasetAttributes datasetAttributes = getNestedShardCodecsAttributes(blockSize);
		writer.createDataset("nestedShards", datasetAttributes);

		final byte[] data = new byte[N];
		Arrays.fill(data, (byte)4);

		writer.writeBlocks("nestedShards", datasetAttributes,
				new ByteArrayDataBlock(blockSize, new long[]{1, 1}, data),
				new ByteArrayDataBlock(blockSize, new long[]{0, 2}, data),
				new ByteArrayDataBlock(blockSize, new long[]{2, 1}, data));

		assertArrayEquals(data, (byte[])writer.readBlock("nestedShards", datasetAttributes, 1, 1).getData());
		assertArrayEquals(data, (byte[])writer.readBlock("nestedShards", datasetAttributes, 0, 2).getData());
		assertArrayEquals(data, (byte[])writer.readBlock("nestedShards", datasetAttributes, 2, 1).getData());
	}

	private DatasetAttributes getNestedShardCodecsAttributes(int[] blockSize) {

		final int[] innerShardSize = new int[]{2 * blockSize[0], 2 * blockSize[1]};
		final int[] shardSize = new int[]{4 * blockSize[0], 4 * blockSize[1]};
		final long[] dimensions = GridIterator.int2long(shardSize);

		// TODO: its not even clear how we build this given
		// 	this constructor. Is the block size of the sharded dataset attributes
		// 	the innermost (block) size or the intermediate shard size?
		// 	probably better to forget about this class - only use DatasetAttributes
		// 	and detect shading in another way
		final ShardingCodec innerShard = new ShardingCodec(innerShardSize,
				new Codec[]{new N5BlockCodec<>()},
				new DeterministicSizeCodec[]{new RawBytes(indexByteOrder), new Crc32cChecksumCodec()},
				IndexLocation.START);

		return new DatasetAttributes(
				dimensions, shardSize, blockSize, DataType.UINT8,
				new ShardingCodec(
						blockSize,
						new Codec[]{innerShard},
						new DeterministicSizeCodec[]{new RawBytes(indexByteOrder), new Crc32cChecksumCodec()},
						IndexLocation.END)
		);
	}

	public static void main(String[] args) {

		final long[] imageSize = new long[]{32, 27};
		final int[] shardSize = new int[]{16, 9};
		final int[] blockSize = new int[]{4, 3};
		final int numBlockElements = Arrays.stream(blockSize).reduce(1, (x, y) -> x * y);

		try (final N5Writer n5 = new N5Factory().openWriter("n5:/tmp/tests/codeReview/sharded.n5")) {

			final DatasetAttributes attributes = getDatasetAttributes(imageSize, shardSize, blockSize);
		}

	}

	private static DatasetAttributes getDatasetAttributes(long[] imageSize, int[] shardSize, int[] blockSize) {

		final DatasetAttributes attributes = new DatasetAttributes(
				imageSize,
				shardSize,
				blockSize,
				DataType.INT32,
				new ShardingCodec(
						blockSize,
						new Codec[]{
								// codecs applied to image data
								new RawBytes(ByteOrder.BIG_ENDIAN),
						},
						new DeterministicSizeCodec[]{
								// codecs applied to the shard index, must not be compressors
								new RawBytes(ByteOrder.LITTLE_ENDIAN),
								new Crc32cChecksumCodec()
						},
						IndexLocation.START
				)
		);
		return attributes;
	}
}
