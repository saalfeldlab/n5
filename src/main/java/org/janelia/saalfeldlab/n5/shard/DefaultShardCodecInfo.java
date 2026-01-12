/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception;
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

	@NameConfig.Parameter(value = "index_location", optional = true)
	private final IndexLocation indexLocation;

	@NameConfig.Parameter
	private CodecInfo[] codecs;

	@NameConfig.Parameter(value = "index_codecs")
	private CodecInfo[] indexCodecs;

	private transient BlockCodecInfo innerBlockCodecInfo;

	private transient DataCodecInfo[] innerDataCodecInfos;

	private transient BlockCodecInfo indexBlockCodecInfo;

	private transient DataCodecInfo[] indexDataCodecInfos;

	DefaultShardCodecInfo() {
		// for serialization
		this(null, null, null, null, null, IndexLocation.END);
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

	private void build() {

		if (innerBlockCodecInfo != null)
			return;

		// sets
		// innerBlockCodecInfo, innerDataCodecInfos
		// indexBlockCodecInfo, indexDataCodecInfos
		// from
		// codecs and indexCodecs

		if (codecs[0] instanceof BlockCodecInfo)
			innerBlockCodecInfo = (BlockCodecInfo)codecs[0];
		else
			throw new N5Exception("Codec at index " + 0 + " must be a BlockCodec.");

		innerDataCodecInfos = new DataCodecInfo[codecs.length - 1];
		for (int i = 1; i < codecs.length; i++)
			innerDataCodecInfos[i - 1] = (DataCodecInfo)codecs[i];

		if (indexCodecs[0] instanceof BlockCodecInfo)
			indexBlockCodecInfo = (BlockCodecInfo)indexCodecs[0];
		else
			throw new N5Exception("Codec at index " + 0 + " must be a BlockCodec.");

		indexDataCodecInfos = new DataCodecInfo[indexCodecs.length - 1];
		for (int i = 1; i < indexCodecs.length; i++)
			indexDataCodecInfos[i - 1] = (DataCodecInfo)indexCodecs[i];
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

		build();

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
