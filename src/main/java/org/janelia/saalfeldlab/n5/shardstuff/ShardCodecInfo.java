package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

public interface ShardCodecInfo extends BlockCodecInfo {

	/**
	 * Chunk size of each shard element (either nested shard or DataBlock)
	 */
	int[] getInnerBlockSize();

	/**
	 * BlockCodec for shard elements (either nested shard or DataBlock)
	 */
	BlockCodecInfo getInnerBlockCodecInfo();

	/**
	 * DataCodecs for inner BlockCodec
	 */
	DataCodecInfo[] getInnerDataCodecInfos();

	/**
	 * BlockCodec for shard index
	 */
	BlockCodecInfo getIndexBlockCodecInfo();

	/**
	 * Deterministic-size DataCodecs for index BlockCodec
	 */
	DataCodecInfo[] getIndexDataCodecInfos();

	IndexLocation getIndexLocation();

	@SuppressWarnings("unchecked")
	@Override
	default RawShardCodec create(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {
		return create(blockSize, codecs);
	}

	RawShardCodec create(int[] blockSize, DataCodecInfo... codecs);
}
