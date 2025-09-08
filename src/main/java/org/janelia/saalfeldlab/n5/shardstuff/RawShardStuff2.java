package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public class RawShardStuff2 {


	public static void main(String[] args) {
		// DataBlocks are 3x3x3
		// Level 1 shards are 6x6x6 (contain 2x2x2 DataBlocks)
		// Level 2 shards are 24x24x24 (contain 4x4x4 Level 1 shards)
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				new int[] {3, 3, 3},
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);
		final ShardCodecInfo c2 = new DefaultShardCodecInfo(
				new int[] {6, 6, 6},
				c1,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.START
		);

		// TODO: This should go into DatasetAttributes.
		//       DatasetAccess<T> replaces DatasetAttributes.blockCodec
		//       DatasetAttributes.getDataAccess() replaces DatasetAttributes.getBlockCodec()
		//       All information required for create(...) is known to DatasetAttributes already.
		final DatasetAccess<byte[]> datasetAccess = create(DataType.INT8,
				new int[] {24, 24, 24},
				c2,
				new DataCodecInfo[] {new RawCompression()});

		// TODO: N5Reader/Writer needs to provide a PositionValueAccess implementation on top of its KVA.
		//       The read/write/deleteBlock methods would getDataAccess() from the DatasetAttributes and call it with that PositionValueAccess.
		final PositionValueAccess store = new TestPositionValueAccess();


		// ---------------------------------------------------------------
		// Some "tests"
		// TODO: Turn into unit tests
		// ---------------------------------------------------------------

		// write some blocks, filled with constant values
		final int[] dataBlockSize = c1.getInnerBlockSize();
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 0, 0}, 1));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 0, 0}, 2));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 1, 0}, 3));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 1, 0}, 4));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {3, 2, 1}, 5));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {8, 4, 1}, 6));

		// verify that the written blocks can be read back with the correct values
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), true, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 1, 0}), true, 3);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 1, 0}), true, 4);
		checkBlock(datasetAccess.readBlock(store, new long[] {3, 2, 1}), true, 5);
		checkBlock(datasetAccess.readBlock(store, new long[] {8, 4, 1}), true, 6);

		// verify that deleting a block removes it from the shard (while other blocks in the same shard are still present)
		datasetAccess.deleteBlock(store, new long[] {0, 0, 0});
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), false, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);

		// if a shard becomes empty the corresponding key should be deleted
		if ( store.get(new long[] {1, 0, 0}) == null ) {
			throw new IllegalStateException("expected non-null readData");
		}
		datasetAccess.deleteBlock(store, new long[] {8, 4, 1});
		if ( store.get(new long[] {1, 0, 0}) != null ) {
			throw new IllegalStateException("expected null readData");
		}

		// deleting a non-existent block should not fail
		datasetAccess.deleteBlock(store, new long[] {0, 0, 8});
	}

	private static void checkBlock(final DataBlock<byte[]> dataBlock, final boolean expectedNonNull, final int expectedFillValue) {

		if (dataBlock == null) {
			if (expectedNonNull) {
				throw new IllegalStateException("expected non-null dataBlock");
			}
		} else {
			if (!expectedNonNull) {
				throw new IllegalStateException("expected null dataBlock");
			}
			final byte[] bytes = dataBlock.getData();
			for (byte b : bytes) {
				if (b != (byte) expectedFillValue) {
					throw new IllegalStateException("expected all values to be " + expectedFillValue);
				}
			}
		}
	}

	private static DataBlock<byte[]> createDataBlock(int[] size, long[] gridPosition, int fillValue) {
		final byte[] bytes = new byte[DataBlock.getNumElements(size)];
		Arrays.fill(bytes, (byte) fillValue);
		return new ByteArrayDataBlock(size, gridPosition, bytes);
	}

	static class TestPositionValueAccess implements PositionValueAccess {

		private static class Key {

			private final long[] data;

			Key(long[] data) {
				this.data = data;
			}

			@Override
			public final boolean equals(final Object o) {
				if (!(o instanceof Key)) {
					return false;
				}
				final Key key = (Key) o;
				return Arrays.equals(data, key.data);
			}

			@Override
			public int hashCode() {
				return Arrays.hashCode(data);
			}
		}

		private final Map<Key, byte[]> map = new HashMap<>();

		@Override
		public ReadData get(final long[] key) {
			final byte[] bytes = map.get(new Key(key));
			return bytes == null ? null : ReadData.from(bytes);
		}

		@Override
		public void put(final long[] key, final ReadData data) {
			final byte[] bytes = data == null ? null : data.allBytes();
			map.put(new Key(key), bytes);
		}

		@Override
		public void remove(final long[] key) throws N5IOException {
			map.remove(new Key(key));
		}
	}


	public interface PositionValueAccess {

		/**
		 * @return ReadData for the given key or {@code null} if the key doesn't exist
		 */
		ReadData get(long[] key) throws N5IOException;

		void put(long[] key, ReadData data) throws N5IOException;

		void remove(long[] key) throws N5IOException;
	}

	public interface DatasetAccess<T> {

		DataBlock<T> readBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;

		void writeBlock(PositionValueAccess kva, DataBlock<T> dataBlock) throws N5IOException;

		void deleteBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;

		// TODO: batch read/write/delete methods
//		List<DataBlock<T>> readBlocks(PositionValueAccess kva, List<NestedPosition> positions);
//		void writeBlocks(PositionValueAccess kva, List<DataBlock<T>> blocks);
	}

	static class ShardedDatasetAccess<T> implements DatasetAccess<T> {

		private final NestedGrid grid;
		private final BlockCodec<?>[] codecs;

		public ShardedDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] codecs) {
			this.grid = grid;
			this.codecs = codecs;
		}

		@Override
		public DataBlock<T> readBlock(final PositionValueAccess kva, final long[] gridPosition) throws N5IOException {
			final NestedPosition position = new NestedPosition(grid, gridPosition);
			return readBlockRecursive(kva.get(position.key()), position, grid.numLevels() - 1);
		}

		private DataBlock<T> readBlockRecursive(
				final ReadData readData,
				final NestedPosition position,
				final int level)
		{
			if (readData == null) {
				return null;
			} else if (level == 0) {
				@SuppressWarnings("unchecked")
				final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
				return codec.decode(readData, position.absolute(0));
			} else {
				@SuppressWarnings("unchecked")
				final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
				final RawShard shard = codec.decode(readData, position.absolute(level)).getData();
				return readBlockRecursive(shard.getElementData(position.relative(level - 1)), position, level - 1);

			}
		}

		@Override
		public void writeBlock(final PositionValueAccess kva, final DataBlock<T> dataBlock) throws N5IOException {
			final NestedPosition position = new NestedPosition(grid, dataBlock.getGridPosition());
			final long[] key = position.key();
			final ReadData existingData = kva.get(key);
			final ReadData modifiedData = writeBlockRecursive(existingData, dataBlock, position, grid.numLevels() - 1);
			kva.put(key, modifiedData);
		}

		@Override
		public void deleteBlock(final PositionValueAccess kva, final long[] gridPosition) throws N5IOException {
			final NestedPosition position = new NestedPosition(grid, gridPosition);
			final long[] key = position.key();
			if ( grid.numLevels() == 1 ) {
				// for non-sharded dataset, don't bother getting the value, just remove the key.
				kva.remove(key);
			} else {
				final ReadData existingData = kva.get(key);
				final ReadData modifiedData = deleteBlockRecursive(existingData, position, grid.numLevels() - 1);
				if ( modifiedData == null ) {
					kva.remove(key);
				} else if ( modifiedData != existingData ) {
					kva.put(key, modifiedData);
				}
			}
		}

		private ReadData deleteBlockRecursive(
				final ReadData existingReadData,
				final NestedPosition position,
				final int level)
		{
			if ( level == 0 || existingReadData == null ) {
				return null;
			} else {
				@SuppressWarnings("unchecked")
				final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
				final long[] gridPos = position.absolute(level);
				final RawShard shard = codec.decode(existingReadData, gridPos).getData();
				final long[] elementPos = position.relative(level - 1);
				final ReadData existingElementData = shard.getElementData(elementPos);
				if ( existingElementData == null ) {
					// The DataBlock (or the whole nested shard containing it) does not exist.
					// This shard remains unchanged.
					return existingReadData;
				} else {
					final ReadData modifiedElementData = deleteBlockRecursive(existingElementData, position, level - 1);
					if ( modifiedElementData == existingElementData ) {
						// The nested shard was not modified.
						// This shard remains unchanged.
						return existingReadData;
					}
					shard.setElementData(modifiedElementData, elementPos);
					if ( modifiedElementData == null ) {
						// The DataBlock or nested shard was removed.
						// Check whether this shard becomes empty.
						if ( shard.index().allElementsNull() ) {
							// This shard is empty and should be removed.
							return null;
						}
					}
					return codec.encode(new RawShardDataBlock(gridPos, shard));
				}
			}
		}

		private ReadData writeBlockRecursive(
				final ReadData existingReadData,
				final DataBlock<T> dataBlock,
				final NestedPosition position,
				final int level)
		{
			if ( level == 0 ) {
				@SuppressWarnings("unchecked")
				final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
				return codec.encode(dataBlock);
			} else {
				@SuppressWarnings("unchecked")
				final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
				final long[] gridPos = position.absolute(level);
				final RawShard shard = existingReadData == null ?
						new RawShard(grid.relativeBlockSize(level)) :
						codec.decode(existingReadData, gridPos).getData();
				final long[] elementPos = position.relative(level - 1);
				final ReadData existingElementData = (level == 1)
						? null // if level == 1, we don't need to extract the nested (DataBlock<T>) ReadData because it will be overridden anyway
						: shard.getElementData(elementPos);
				final ReadData modifiedElementData = writeBlockRecursive(existingElementData, dataBlock, position, level - 1);
				shard.setElementData(modifiedElementData, elementPos);
				return codec.encode(new RawShardDataBlock(gridPos, shard));
			}
		}
	}


	public static <T> DatasetAccess<T> create(
			final DataType dataType,
			int[] blockSize,
			BlockCodecInfo blockCodecInfo,
			DataCodecInfo[] dataCodecInfos
	) {
		final int m = nestingDepth(blockCodecInfo);

		// There are m codecs: 1 DataBlock codecs, and m-1 shard codecs.
		// The inner-most codec (the DataBlock codec) is at index 0.
		final BlockCodec<?>[] blockCodecs = new BlockCodec[m];
		final int[][] blockSizes = new int[m][];

		for (int l = m - 1; l >= 0; --l) {
			blockCodecs[l] = blockCodecInfo.create(dataType, blockSize, dataCodecInfos);
			blockSizes[l] = blockSize;
			if ( l > 0 ) {
				final ShardCodecInfo info = (ShardCodecInfo) blockCodecInfo;
				blockCodecInfo = info.getInnerBlockCodecInfo();
				dataCodecInfos = info.getInnerDataCodecInfos();
				blockSize = info.getInnerBlockSize();
			}
		}

		return new ShardedDatasetAccess<>(new NestedGrid(blockSizes), blockCodecs);
	}

	private static int nestingDepth(BlockCodecInfo info) {
		if (info instanceof ShardCodecInfo) {
			return 1 + nestingDepth(((ShardCodecInfo) info).getInnerBlockCodecInfo());
		} else {
			return 1;
		}
	}

	public static class DefaultShardCodecInfo implements ShardCodecInfo {

		@Override
		public String getType() {
			return "ShardingCodec";
		}

		private final int[] innerBlockSize;
		private final BlockCodecInfo innerBlockCodecInfo;
		private final DataCodecInfo[] innerDataCodecInfos;
		private final BlockCodecInfo indexBlockCodecInfo;
		private final DataCodecInfo[] indexDataCodecInfos;
		private final IndexLocation indexLocation;

		public DefaultShardCodecInfo(
				final int[] innerBlockSize,
				final BlockCodecInfo innerBlockCodecInfo,
				final DataCodecInfo[] innerDataCodecInfos,
				final BlockCodecInfo indexBlockCodecInfo,
				final DataCodecInfo[] indexDataCodecInfos,
				final IndexLocation indexLocation) {
			this.innerBlockSize = innerBlockSize;
			this.innerBlockCodecInfo = innerBlockCodecInfo;
			this.innerDataCodecInfos = innerDataCodecInfos;
			this.indexBlockCodecInfo = indexBlockCodecInfo;
			this.indexDataCodecInfos = indexDataCodecInfos;
			this.indexLocation = indexLocation;
		}

		@Override
		public int[] getInnerBlockSize() {
			return innerBlockSize;
		}

		@Override
		public BlockCodecInfo getInnerBlockCodecInfo() {
			return innerBlockCodecInfo;
		}

		@Override
		public DataCodecInfo[] getInnerDataCodecInfos() {
			return innerDataCodecInfos;
		}

		@Override
		public BlockCodecInfo getIndexBlockCodecInfo() {
			return indexBlockCodecInfo;
		}

		@Override
		public DataCodecInfo[] getIndexDataCodecInfos() {
			return indexDataCodecInfos;
		}

		@Override
		public IndexLocation getIndexLocation() {
			return indexLocation;
		}

		@Override
		public RawShardCodec create(final int[] blockSize, final DataCodecInfo... codecs) {

			// Number of elements (DataBlocks, nested shards) in each dimension per shard.
			final int[] size = new int[blockSize.length];
			// blockSize argument is number of pixels in the shard
			// innerBlockSize is number of pixels in each shard element (nested shard or DataBlock)
			Arrays.setAll(size, d -> blockSize[d] / innerBlockSize[d]);

			final BlockCodec<long[]> indexCodec = indexBlockCodecInfo.create(
					DataType.UINT64,
					ShardIndex.blockSizeFromIndexSize(size),
					indexDataCodecInfos);

			return new RawShardCodec(size, indexLocation, indexCodec);
		}
	}
}
