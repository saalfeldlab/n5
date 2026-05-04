package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;

public interface ShardCodecInfo extends BlockCodecInfo {

	/**
	 * Size in pixels of each shard element (either nested shard or chunk)
	 *
	 * @return the size of each shard element
	 */
	int[] getInnerBlockSize();

	/**
	 *
	 * @return the collection of DatasetCodecInfo applied to data blocks for this shard
	 */
	DatasetCodecInfo[] getInnerDatasetCodecInfos();

	/**
	 * BlockCodecInfo for shard elements (either nested shard or DataBlock)
	 *
	 * @return the BlockCodecInfo for DataBlocks in this shard
	 */
	BlockCodecInfo getInnerBlockCodecInfo();

	/**
	 * @return the collection of DataCodecInfos applied to data blocks for this
	 *         shard.
	 */
	DataCodecInfo[] getInnerDataCodecInfos();

	/**
	 * BlockCodec for shard index
	 *
	 * @return the BlockCodecInfo for this shard's index
	 */
	BlockCodecInfo getIndexBlockCodecInfo();

	/**
	 * Deterministic-size DataCodecs for index BlockCodec
	 *
	 * @return the collection of DataCodecInfos for this shard's index
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
