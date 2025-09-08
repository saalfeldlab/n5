package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardCodec;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public class RawShardStuff2 {








	public interface DatasetAccess<T> {

		DataBlock<T> readBlock(KeyValueAccess kva, long[] gridPosition) throws N5IOException;

		void writeBlock(KeyValueAccess kva, DataBlock<T> dataBlock) throws N5IOException;

//		List<DataBlock<T>> readBlocks(List<NestedPosition> positions);
//		void writeBlocks(List<DataBlock<T>> blocks);
	}

	public static class ShardedDatasetAccess<T> implements DatasetAccess<T> {

		private final NestedGrid grid;

		public ShardedDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] blockCodecs) {
			this.grid = grid;
		}

		@Override
		public DataBlock<T> readBlock(final KeyValueAccess kva, final long[] gridPosition) throws N5IOException {
			throw new UnsupportedOperationException("TODO");
		}

		@Override
		public void writeBlock(final KeyValueAccess kva, final DataBlock<T> dataBlock) throws N5IOException {
			throw new UnsupportedOperationException("TODO");
		}
	}


	static <T> DatasetAccess<T> create(
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



	public static class DummyDataBlockCodecInfo implements BlockCodecInfo {

		@Override
		public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecs) {
			return null;
		}

		@Override
		public String getType() {
			return "DummyDataBlockCodecInfo";
		}
	}


}
