package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShard;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardCodec;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardDataBlock;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public class RawShardStuff2 {







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
