package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class ShardTest {

	private static final N5FSTest tempN5Factory = new N5FSTest();

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
		final Object[][] paramArray = new Object[params.size()][];
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

	private ShardedDatasetAttributes getTestAttributes(long[] dimensions, int[] shardSize, int[] blockSize) {
		return new ShardedDatasetAttributes(
				dimensions,
				shardSize,
				blockSize,
				DataType.UINT8,
				new Codec[]{new N5BlockCodec(dataByteOrder)}, // , new GzipCompression(4)},
				new DeterministicSizeCodec[]{new BytesCodec(indexByteOrder), new Crc32cChecksumCodec()},
				indexLocation
		);
	}

	private ShardedDatasetAttributes getTestAttributes() {
			return getTestAttributes(new long[]{8, 8}, new int[]{4, 4}, new int[]{2, 2});
	}

	@Test
	public void writeReadBlocksTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();
		final ShardedDatasetAttributes datasetAttributes = getTestAttributes(
				new long[]{24,24},
				new int[]{8,8},
				new int[]{2,2}
		);

		writer.createDataset("shard", datasetAttributes);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final int numElements = blockSize[0] * blockSize[1];

		final byte[] data = new byte[numElements];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)((100) + (10) + i);
		}


		writer.writeBlocks(
				"shard",
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0,0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{0,1}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1,0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{1,1}, data),

				/* shard (1, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{4,0}, data),
				new ByteArrayDataBlock(blockSize, new long[]{5,0}, data),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{11,11}, data)
		);

		final KeyValueAccess kva = ((N5KeyValueWriter)writer).getKeyValueAccess();

		final String[][] keys = new String[][]{
				{"shard", "0", "0"},
				{"shard", "1", "0"},
				{"shard", "2", "2"}
		};
		for (String[] key : keys) {
			final String shard = kva.compose(writer.getURI(), key);
			Assert.assertTrue("Shard at" + Arrays.toString(key) + "Does not exist", kva.exists(shard));
		}

		final long[][] blockIndices = new long[][]{ {0,0}, {0,1}, {1,0}, {1,1}, {4,0}, {5,0}, {11,11}};
		for (long[] blockIndex : blockIndices) {
			final DataBlock<?> block = writer.readBlock("shard", datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
		}

		final byte[] data2 = new byte[numElements];
		for (int i = 0; i < data2.length; i++) {
			data2[i] = (byte)(10 + i);
		}
		writer.writeBlocks(
				"shard",
				datasetAttributes,
				/* shard (0, 0) */
				new ByteArrayDataBlock(blockSize, new long[]{0,0}, data2),
				new ByteArrayDataBlock(blockSize, new long[]{1,1}, data2),

				/* shard (0, 1) */
				new ByteArrayDataBlock(blockSize, new long[]{0,4}, data2),
				new ByteArrayDataBlock(blockSize, new long[]{0,5}, data2),

				/* shard (2, 2) */
				new ByteArrayDataBlock(blockSize, new long[]{10,10}, data2)
		);

		final String[][] keys2 = new String[][]{
				{"shard", "0", "0"},
				{"shard", "1", "0"},
				{"shard", "0", "1"},
				{"shard", "2", "2"}
		};
		for (String[] key : keys2) {
			final String shard = kva.compose(writer.getURI(), key);
			Assert.assertTrue("Shard at" + Arrays.toString(key) + "Does not exist", kva.exists(shard));
		}

		final long[][] oldBlockIndices = new long[][]{{0,1}, {1,0}, {4,0}, {5,0}, {11,11}};
		for (long[] blockIndex : oldBlockIndices) {
			final DataBlock<?> block = writer.readBlock("shard", datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());
		}

		final long[][] newBlockIndices = new long[][]{{0,0}, {1,1}, {0,4}, {0,5}, {10,10}};
		for (long[] blockIndex : newBlockIndices) {
			final DataBlock<?> block = writer.readBlock("shard", datasetAttributes, blockIndex);
			Assert.assertArrayEquals("Read from shard doesn't match", data2, (byte[])block.getData());
		}
	}

	@Test
	public void writeReadBlockTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();
		final ShardedDatasetAttributes datasetAttributes = getTestAttributes();

		writer.createDataset("shard", datasetAttributes);
		writer.deleteBlock("shard", 0, 0);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenBlocks = new HashMap<>();

		for (int idx1 = 1; idx1 >= 0; idx1--) {
			for (int idx2 = 1; idx2 >= 0; idx2--) {
				final long[] gridPosition = {idx1, idx2};
				final DataBlock<?> dataBlock = dataType.createDataBlock(blockSize, gridPosition, numElements);
				byte[] data = (byte[])dataBlock.getData();
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
				}
				writer.writeBlock("shard", datasetAttributes, dataBlock);

				final DataBlock<?> block = writer.readBlock("shard", datasetAttributes, gridPosition);
				Assert.assertArrayEquals("Read from shard doesn't match", data, (byte[])block.getData());

				for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
					final long[] otherGridPosition = entry.getKey();
					final byte[] otherData = entry.getValue();
					final DataBlock<?> otherBlock = writer.readBlock("shard", datasetAttributes, otherGridPosition);
					Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
				}

				writtenBlocks.put(gridPosition, data);
			}
		}
	}

	@Test
	public void writeReadShardTest() {

		final N5Writer writer = tempN5Factory.createTempN5Writer();

		final ShardedDatasetAttributes datasetAttributes = new ShardedDatasetAttributes(
				new long[]{4, 4},
				new int[]{4, 4},
				new int[]{2, 2},
				DataType.UINT8,
				new Codec[]{new N5BlockCodec(dataByteOrder)},
				new DeterministicSizeCodec[]{new BytesCodec(indexByteOrder), new Crc32cChecksumCodec()},
				indexLocation
		);
		writer.createDataset("wholeShard", datasetAttributes);
		writer.deleteBlock("wholeShard", 0, 0);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		final HashMap<long[], byte[]> writtenBlocks = new HashMap<>();

		final InMemoryShard<byte[]> shard = new InMemoryShard<byte[]>(datasetAttributes, new long[]{0, 0});

		for (int idx1 = 1; idx1 >= 0; idx1--) {
			for (int idx2 = 1; idx2 >= 0; idx2--) {
				final long[] gridPosition = {idx1, idx2};
				final DataBlock<?> dataBlock = dataType.createDataBlock(blockSize, gridPosition, numElements);
				byte[] data = (byte[])dataBlock.getData();
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
				}
				shard.addBlock((DataBlock<byte[]>)dataBlock);
				writtenBlocks.put(gridPosition, data);
			}
		}

		writer.writeShard("wholeShard", datasetAttributes, shard);

		for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
			final long[] otherGridPosition = entry.getKey();
			final byte[] otherData = entry.getValue();
			final DataBlock<?> otherBlock = writer.readBlock("wholeShard", datasetAttributes, otherGridPosition);
			Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
		}
	}

}
