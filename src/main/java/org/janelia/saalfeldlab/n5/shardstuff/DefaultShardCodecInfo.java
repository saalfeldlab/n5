package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

/**
 * Default (and probably only) implementation of {@link ShardCodecInfo}.
 */
// TODO rename?
public class DefaultShardCodecInfo implements ShardCodecInfo {

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
