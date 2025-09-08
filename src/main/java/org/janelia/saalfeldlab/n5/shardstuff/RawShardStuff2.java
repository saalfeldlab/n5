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
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShard;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardCodec;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardDataBlock;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.ShardCodecInfo;
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

		final DatasetAccess<byte[]> datasetAccess = create(DataType.INT8,
				new int[] {24, 24, 24},
				c2,
				new DataCodecInfo[] {new RawCompression()});

		final PositionValueAccess store = new TestPositionValueAccess();

		final int[] dataBlockSize = c1.getInnerBlockSize();

		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 0, 0}, 1));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 0, 0}, 2));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {0, 1, 0}, 3));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {1, 1, 0}, 4));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {3, 2, 1}, 5));
		datasetAccess.writeBlock(store, createDataBlock(dataBlockSize, new long[] {8, 4, 1}, 6));

		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), true, 1);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);
		checkBlock(datasetAccess.readBlock(store, new long[] {0, 1, 0}), true, 3);
		checkBlock(datasetAccess.readBlock(store, new long[] {1, 1, 0}), true, 4);
		checkBlock(datasetAccess.readBlock(store, new long[] {3, 2, 1}), true, 5);
		checkBlock(datasetAccess.readBlock(store, new long[] {8, 4, 1}), true, 6);
	}

	private static void checkBlock(final DataBlock<byte[]> dataBlock, final boolean expectedNonNull, final int expectedFillValue) {

		if (dataBlock == null && expectedNonNull) {
			throw new IllegalStateException("expected non-null dataBlock");
		}

		final byte[] bytes = dataBlock.getData();
		for (byte b : bytes) {
			if (b != (byte) expectedFillValue) {
				throw new IllegalStateException("expected all values to be " + expectedFillValue);
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

		// TODO: batch read/write methods
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
			// TODO
			//  [ ] private ReadData deleteBlockRecursive(...)
			//      [ ] in principle similar to writeBlockRecursive --> we need to decode/modify/re-encode shards
			//      [ ] level == 0 should return null (no block to encode)
			//     	    [ ] when receiving null for non-sharded dataset (probably sharded too) deleteBlock() should remove the key
			//      [ ] if at any point existingReadData is already null, do nothing
			//   	   --> maybe signal that by returning the same existing ReadData?
			// 		[ ] when removing the last remaining block in a Shard, remove the shard
			//         --> this is signalled by returning null from nested call
			//             if this is a Shard, we will setElementData(null)
			//             We then need to check whether the shard became empty by inspecting the shard index and counting non-null values
			//      [ ] for sharded datasets, we don't even need to recursively descend to level 0. just immediately setElementData(null)
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
