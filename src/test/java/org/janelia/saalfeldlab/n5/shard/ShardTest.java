package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSTest;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.DatasetAttributes.DatasetAttributesAdapter;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.io.File;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ShardTest {

	private static final boolean LOCAL_DEBUG = false;

	private static final N5FSTest tempN5Factory = new N5FSTest() {

		@Override public N5Writer createTempN5Writer() {

			if (LOCAL_DEBUG) {
				final N5Writer writer = new ShardedN5Writer("src/test/resources/test.n5");
				writer.remove(""); // Clear old when starting new test
				return writer;
			}

			final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
			try {
				String uri = new URI("file", null, basePath, null).toString();
				return new ShardedN5Writer(uri);
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

	public static GsonBuilder gsonBuilder() {
		return new GsonBuilder();
	}

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

		return new DatasetAttributes(
				dimensions,
				shardSize,
				blockSize,
				DataType.UINT8,
				new ShardingCodec(
						blockSize,
						new CodecInfo[]{new N5BlockCodecInfo()},
						new CodecInfo[]{new RawBlockCodecInfo(), new Crc32cChecksumCodec()},
						indexLocation
				)
		);
	}

	private DatasetAttributes getTestAttributes() {

		return getTestAttributes(new long[]{8, 8}, new int[]{4, 4}, new int[]{2, 2});
	}

	private DatasetAttributes getTestAttributes3d() {

		final int[] blockSize = {33, 22, 11};
		final int[] shardSize = {blockSize[0] * 2, blockSize[1] * 2, blockSize[2] * 2};
		return getTestAttributes(new long[]{10, 20, 30}, shardSize, blockSize);
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
		writer.deleteBlock(dataset, 0, 0); //FIXME Caleb: We are abusing this here. It shouldn't delete the entire shard..

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

		writer.writeShard(dataset, datasetAttributes, shard);

		for (Map.Entry<long[], byte[]> entry : writtenBlocks.entrySet()) {
			final long[] otherGridPosition = entry.getKey();
			final byte[] otherData = entry.getValue();
			final DataBlock<?> otherBlock = writer.readBlock(dataset, datasetAttributes, otherGridPosition);
			Assert.assertArrayEquals("Read prior write from shard no loner matches", otherData, (byte[])otherBlock.getData());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public <T> void testShardDelete() {

		final Random rnd = new Random(88);
		try (GsonKeyValueN5Writer writer = (GsonKeyValueN5Writer)tempN5Factory.createTempN5Writer()) {
			final String datasetName = "testShardDelete";

			// Create a sharded dataset
			final DatasetAttributes datasetAttributes = getTestAttributes3d();
			int[] blockSize = datasetAttributes.getBlockSize();
			writer.createDataset(datasetName, datasetAttributes);

			final int blockNumElements = Arrays.stream(blockSize).reduce(1, (x,y) -> x*y);
			final byte[] byteBlock = new byte[blockNumElements];
			rnd.nextBytes(byteBlock);

			// Create blocks in different shards
			final long[] shardPosition1 = {0, 0, 0};
			final long[] shardPosition2 = {0, 0, 1}; // Different shard in z dimension
			final long[] shardPositionNonExistent = {0, 0, 2}; // Different shard in z dimension

			// Create blocks within the first shard
			final long[] blockPosition1 = {0, 0, 0};
			final long[] blockPosition2 = {1, 0, 0};
			final long[] blockPosition3 = {0, 1, 0};

			// Create a block in the second shard
			final long[] blockPosition4 = {0, 0, 2}; // This will be in
														// shardPosition2

			final ByteArrayDataBlock block1 = new ByteArrayDataBlock(blockSize, blockPosition1, byteBlock);
			final ByteArrayDataBlock block2 = new ByteArrayDataBlock(blockSize, blockPosition2, byteBlock);
			final ByteArrayDataBlock block3 = new ByteArrayDataBlock(blockSize, blockPosition3, byteBlock);
			final ByteArrayDataBlock block4 = new ByteArrayDataBlock(blockSize, blockPosition4, byteBlock);

			// Write blocks to create shards
			writer.writeBlocks(datasetName, datasetAttributes, block1, block2, block3, block4);

			// Verify shards exist
			final Shard<T> shard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			final Shard<T> shard2 = writer.readShard(datasetName, datasetAttributes, shardPosition2);
			final Shard<T> shardDNE = writer.readShard(datasetName, datasetAttributes, shardPositionNonExistent);
			assertNotNull("Shard 1 should exist", shard1);
			assertNotNull("Shard 2 should exist", shard2);
			assertNull("Shard 3 should not exist", shardDNE);
			assertEquals("Shard 1 should contain 3 blocks", 3, shard1.getBlocks().size());
			assertEquals("Shard 2 should contain 1 block", 1, shard2.getBlocks().size());

			// Test deleteShard
			boolean deleted1 = writer.deleteShard(datasetName, shardPosition1);
			assertTrue("deleteShard should return true when shard exists", deleted1);

			// Verify shard is deleted
			final Shard<T> deletedShard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			assertNull("Shard 1 should be deleted", deletedShard1);

			// Verify blocks in the deleted shard are gone
			assertNull("Block 1 should be deleted", writer.readBlock(datasetName, datasetAttributes, blockPosition1));
			assertNull("Block 2 should be deleted", writer.readBlock(datasetName, datasetAttributes, blockPosition2));
			assertNull("Block 3 should be deleted", writer.readBlock(datasetName, datasetAttributes, blockPosition3));

			// Verify other shard is unaffected
			final Shard<T> stillExistingShard2 = writer.readShard(datasetName, datasetAttributes, shardPosition2);
			assertNotNull("Shard 2 should still exist", stillExistingShard2);
			assertNotNull("Block 4 should still exist", writer.readBlock(datasetName, datasetAttributes, blockPosition4));

			// Test deleting non-existent shard
			boolean deletedAgain = writer.deleteShard(datasetName, shardPosition1);
			assertFalse("deleteShard should return false when shard doesn't exist", deletedAgain);

			// Test deleting shard at invalid position
			final long[] invalidShardPosition = {100, 100, 100};
			boolean deletedInvalid = writer.deleteShard(datasetName, invalidShardPosition);
			assertFalse("deleteShard should return false for non-existent shard position", deletedInvalid);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public <T> void testShardedBlockDelete() {

		final Random rnd = new Random(88);
		try (GsonKeyValueN5Writer writer = (GsonKeyValueN5Writer)tempN5Factory.createTempN5Writer()) {
			final String datasetName = "testShardBlockDelete";

			// Create a sharded dataset
			final DatasetAttributes datasetAttributes = getTestAttributes3d();
			int[] blockSize = datasetAttributes.getBlockSize();
			writer.createDataset(datasetName, datasetAttributes);

			final int blockNumElements = Arrays.stream(blockSize).reduce(1, (x,y) -> x*y);
			final byte[] byteBlock = new byte[blockNumElements];
			rnd.nextBytes(byteBlock);

			// Create blocks in different shards
			final long[] shardPosition1 = {0, 0, 0};
			final long[] shardPosition2 = {0, 0, 1}; // Different shard in z dimension
			final long[] shardPositionNonExistent = {0, 0, 2}; // Different shard in z dimension

			// Create blocks within the first shard
			final long[] blockPosition1 = {0, 0, 0};
			final long[] blockPosition2 = {1, 0, 0};
			final long[] blockPosition3 = {0, 1, 0};

			// Create a block in the second shard
			final long[] blockPosition4 = {0, 0, 2}; // This will be in shardPosition2

			final ByteArrayDataBlock block1 = new ByteArrayDataBlock(blockSize, blockPosition1, byteBlock);
			final ByteArrayDataBlock block2 = new ByteArrayDataBlock(blockSize, blockPosition2, byteBlock);
			final ByteArrayDataBlock block3 = new ByteArrayDataBlock(blockSize, blockPosition3, byteBlock);
			final ByteArrayDataBlock block4 = new ByteArrayDataBlock(blockSize, blockPosition4, byteBlock);

			// Write blocks to create shards
			writer.writeBlocks(datasetName, datasetAttributes, block1, block2, block3, block4);

			// Verify shards exist
			Shard<T> shard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			Shard<T> shard2 = writer.readShard(datasetName, datasetAttributes, shardPosition2);
			Shard<T> shardDNE = writer.readShard(datasetName, datasetAttributes, shardPositionNonExistent);
			assertNotNull("Shard 1 should exist", shard1);
			assertNotNull("Shard 2 should exist", shard2);
			assertNull("Shard 3 should not exist", shardDNE);
			assertEquals("Shard 1 should contain 3 blocks", 3, shard1.getBlocks().size());
			assertEquals("Shard 2 should contain 1 block", 1, shard2.getBlocks().size());

			// Test delete one block from a multi-block shard
			boolean deleted1 = writer.deleteBlock(datasetName, blockPosition1);
			assertTrue("deleteBlock1", deleted1);
			shard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			assertNotNull("Shard 1 should still exist", shard1);
			DataBlock<Object> blk1Read = writer.readBlock(datasetName, datasetAttributes, blockPosition1);
			DataBlock<Object> blk2Read = writer.readBlock(datasetName, datasetAttributes, blockPosition2);
			DataBlock<Object> blk3Read = writer.readBlock(datasetName, datasetAttributes, blockPosition3);
			assertNull("Block 1 should not exist", blk1Read);
			assertNotNull("Block 2 should exist", blk2Read);
			assertNotNull("Block 3 should exist", blk3Read);

			// Test delete one block from a multi-block shard
			boolean deleted2 = writer.deleteBlock(datasetName, blockPosition2);
			assertTrue("deleteBlock2", deleted2);
			shard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			assertNotNull("Shard 1 should still exist", shard1);
			blk2Read = writer.readBlock(datasetName, datasetAttributes, blockPosition2);
			blk3Read = writer.readBlock(datasetName, datasetAttributes, blockPosition3);
			assertNull("Block 2 should not exist", blk2Read);
			assertNotNull("Block 3 should exist", blk3Read);

			// Test delete last block from a multi-block shard
			boolean deleted3 = writer.deleteBlock(datasetName, blockPosition3);
			assertTrue("deleteBlock3", deleted3);
			shard1 = writer.readShard(datasetName, datasetAttributes, shardPosition1);
			assertNull("Shard 1 should not exist", shard1);
			blk3Read = writer.readBlock(datasetName, datasetAttributes, blockPosition3);
			assertNull("Block 3 should not exist", blk3Read);

			// Test delete last block from a multi-block shard
			boolean deleted4 = writer.deleteBlock(datasetName, blockPosition4);
			assertTrue("deleteBlock4", deleted4);
			shard2 = writer.readShard(datasetName, datasetAttributes, shardPosition2);
			assertNull("Shard 2 should not exist", shard2);
			DataBlock<Object> blk4Read = writer.readBlock(datasetName, datasetAttributes, blockPosition4);
			assertNull("Block 2 should not exist", blk4Read);
		}
	}

	@Test
	@Ignore("Nested sharding not supported ")
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
				new ByteArrayDataBlock(blockSize, new long[]{2, 1}, data)
		);

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
				new CodecInfo[]{new N5BlockCodecInfo()},
				new DeterministicSizeCodecInfo[]{new RawBlockCodecInfo(indexByteOrder), new Crc32cChecksumCodec()},
				IndexLocation.START);

		return new DatasetAttributes(
				dimensions, shardSize, blockSize, DataType.UINT8,
				new ShardingCodec(
						blockSize,
						new CodecInfo[]{innerShard},
						new DeterministicSizeCodecInfo[]{new RawBlockCodecInfo(indexByteOrder), new Crc32cChecksumCodec()},
						IndexLocation.END)
		);
	}

	/**
	 * An N5Writer that serializing the sharding codecs, enabling testing of
	 * shard functionality, despite the fact that the N5 format does not support
	 * sharding.
	 */
	public static class ShardedN5Writer extends N5FSWriter {

		Gson gson;

		TestDatasetAttributesAdapter adapter = new TestDatasetAttributesAdapter();

		public ShardedN5Writer(String basePath) {

			this(basePath, new GsonBuilder());
		}

		public ShardedN5Writer(String basePath, GsonBuilder gsonBuilder) {

			super(basePath);
			gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
			gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
			gsonBuilder.registerTypeHierarchyAdapter(DatasetAttributes.class, new TestDatasetAttributesAdapter());
			gsonBuilder.registerTypeHierarchyAdapter(ByteOrder.class, RawBlockCodecInfo.byteOrderAdapter);
			gsonBuilder.registerTypeHierarchyAdapter(ShardingCodec.IndexLocation.class, ShardingCodec.indexLocationAdapter);
			gsonBuilder.disableHtmlEscaping();
			gson = gsonBuilder.create();
		}

		@Override
		public Gson getGson() {

			// the super constructor needs the gson instance, unfortunately
			return gson == null ? super.gson : gson;
		}

		@Override
		public DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

			final JsonDeserializationContext context = new JsonDeserializationContext() {

				@Override
				public <T> T deserialize(JsonElement json, Type typeOfT) throws JsonParseException {

					return getGson().fromJson(json, typeOfT);
				}
			};

			return adapter.deserialize(attributes, DatasetAttributes.class, context);
		}
	}

	public static class TestDatasetAttributesAdapter extends DatasetAttributesAdapter {

		@Override
		public DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (json == null || !json.isJsonObject())
				return null;
			final JsonObject obj = json.getAsJsonObject();
			final boolean validKeySet = obj.has(DatasetAttributes.DIMENSIONS_KEY)
					&& obj.has(DatasetAttributes.BLOCK_SIZE_KEY)
					&& obj.has(DatasetAttributes.DATA_TYPE_KEY)
					&& (obj.has(DatasetAttributes.CODEC_KEY) || obj.has(DatasetAttributes.COMPRESSION_KEY));

			if (!validKeySet)
				return null;

			final long[] dimensions = context.deserialize(obj.get(DatasetAttributes.DIMENSIONS_KEY), long[].class);
			final int[] blockSize = context.deserialize(obj.get(DatasetAttributes.BLOCK_SIZE_KEY), int[].class);
			final int[] shardSize = context.deserialize(obj.get(DatasetAttributes.SHARD_SIZE_KEY), int[].class);

			final DataType dataType = context.deserialize(obj.get(DatasetAttributes.DATA_TYPE_KEY), DataType.class);

			final CodecInfo[] codecs;
			if (obj.has(DatasetAttributes.CODEC_KEY)) {
				codecs = context.deserialize(obj.get(DatasetAttributes.CODEC_KEY), CodecInfo[].class);
			} else if (obj.has(DatasetAttributes.COMPRESSION_KEY)) {
				final Compression compression
						= CompressionAdapter.getJsonAdapter().deserialize(obj.get(DatasetAttributes.COMPRESSION_KEY), Compression.class, context);
				codecs = new CodecInfo[]{compression};
			} else {
				return null;
			}
			final BlockCodecInfo blockCodecInfo;
			final DataCodecInfo[] dataCodecInfos;
			if (codecs[0] instanceof BlockCodecInfo) {
				blockCodecInfo  = (BlockCodecInfo)codecs[0];
				dataCodecInfos = new DataCodecInfo[codecs.length - 1];
				System.arraycopy(codecs, 1, dataCodecInfos, 0, dataCodecInfos.length);
			} else {
				blockCodecInfo = null;
				dataCodecInfos = (DataCodecInfo[])codecs;
			}
			return new DatasetAttributes(dimensions, shardSize, blockSize, dataType, blockCodecInfo, dataCodecInfos);
		}

		@Override
		public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DatasetAttributes.DIMENSIONS_KEY, context.serialize(src.getDimensions()));
			obj.add(DatasetAttributes.BLOCK_SIZE_KEY, context.serialize(src.getBlockSize()));

			final int[] shardSize = src.getShardSize();
			if (shardSize != null) {
				obj.add(DatasetAttributes.SHARD_SIZE_KEY, context.serialize(shardSize));
			}

			obj.add(DatasetAttributes.DATA_TYPE_KEY, context.serialize(src.getDataType()));
			obj.add(DatasetAttributes.CODEC_KEY, context.serialize(concatenateCodecs(src)));
			return obj;
		}
	}

	private static CodecInfo[] concatenateCodecs(final DatasetAttributes attributes) {

		final DataCodecInfo[] byteCodecs = attributes.getDataCodecInfos();
		final BlockCodecInfo blockCodecInfo = attributes.getBlockCodecInfo();
		final CodecInfo[] allCodecs = new CodecInfo[byteCodecs.length + 1];
		allCodecs[0] = blockCodecInfo;
		System.arraycopy(byteCodecs, 0, allCodecs, 1, byteCodecs.length);

		return allCodecs;
	}

}
