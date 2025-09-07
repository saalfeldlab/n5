package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.List;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.RawShardCodec;
import org.janelia.saalfeldlab.n5.shardstuff.RawShardStuff.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public class RawShardStuff2 {








	// TODO: rename?
	public interface Sharding<T> {

		List<DataBlock<T>> readBlocks(ReadData readData, List<Nesting.NestedPosition> positions);

		ReadData writeBlocks(ReadData readData, List<DataBlock<T>> blocks);
	}




	public static class DummyShardCodecInfo implements ShardCodecInfo {

		private final int[] innerBlockSize;
		private final BlockCodecInfo innerBlockCodecInfo;
		private final DataCodecInfo[] innerDataCodecInfos;
		private final BlockCodecInfo indexBlockCodecInfo;
		private final DataCodecInfo[] indexDataCodecInfos;
		private final IndexLocation indexLocation;

		public DummyShardCodecInfo(
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
			return null;
		}

		@Override
		public String getType() {
			return "DummyShardCodec";
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
