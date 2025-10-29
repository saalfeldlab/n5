package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;

/**
 * Default (and probably only) implementation of {@link ShardCodecInfo}.
 */
@NameConfig.Name(value = "sharding_indexed")
public class DefaultShardCodecInfo implements ShardCodecInfo {

	@Override
	public String getType() {
		return "sharding_indexed";
	}

	@N5Annotations.ReverseArray
	@NameConfig.Parameter(value = "chunk_shape")
	private final int[] innerBlockSize;

	@NameConfig.Parameter(value = "index_location")
	private final IndexLocation indexLocation;

	@NameConfig.Parameter
	private CodecInfo[] codecs;

	@NameConfig.Parameter(value = "index_codecs")
	private CodecInfo[] indexCodecs;

	private transient final BlockCodecInfo innerBlockCodecInfo;

	private transient final DataCodecInfo[] innerDataCodecInfos;

	private transient final BlockCodecInfo indexBlockCodecInfo;

	private transient final DataCodecInfo[] indexDataCodecInfos;

	DefaultShardCodecInfo() {
		// for serialization
		this(null, null, null, null, null, null);
	}

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

		codecs = concatenateCodecs(innerBlockCodecInfo, innerDataCodecInfos);
		indexCodecs = concatenateCodecs(indexBlockCodecInfo, indexDataCodecInfos);
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

	public CodecInfo[] getCodecs() {
		return codecs;
	}

	public CodecInfo[] getIndexCodecs() {
		return indexCodecs;
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

	private static CodecInfo[] concatenateCodecs(BlockCodecInfo blkInfo, DataCodecInfo[] dataInfos) {

		if (dataInfos == null) {
			return new CodecInfo[]{blkInfo};
		}

		final CodecInfo[] allCodecs = new CodecInfo[dataInfos.length + 1];
		allCodecs[0] = blkInfo;
		System.arraycopy(dataInfos, 0, allCodecs, 1, dataInfos.length);

		return allCodecs;
	}
}
