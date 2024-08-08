package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.Codec;
import org.janelia.saalfeldlab.n5.codec.ByteStreamCodec;
import org.janelia.saalfeldlab.n5.dataset.DatasetToByteStream;
import org.janelia.saalfeldlab.n5.serialization.N5NameConfig;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

@N5NameConfig.Type("sharding_indexed")
@N5NameConfig.Prefix("codec")
public class ShardingCodec implements DatasetToByteStream {

	private static final long serialVersionUID = -5879797314954717810L;

	private final int[] blockSize;

	private final Codec[] codecs;

	private final Codec[] indexCodecs;

	private IndexLocation indexLocation;

	public ShardingCodec(
			final int[] blockSize,
			final Codec[] codecs,
			final Codec[] indexCodecs,
			final IndexLocation indexLocation) {

		this.blockSize = blockSize;
		this.codecs = codecs;
		this.indexCodecs = indexCodecs;
		this.indexLocation = indexLocation;
	}

	private ShardingCodec() {

		this.blockSize = null;
		this.codecs = null;
		this.indexCodecs = null;
		this.indexLocation = null;
	}

	public static boolean isShardingCodec(final ByteStreamCodec codec) {

		return codec instanceof ShardingCodec;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public Codec[] getCodecs() {

		return codecs;
	}

	public Codec[] getIndexCodecs() {

		return indexCodecs;
	}

	public IndexLocation getIndexLocation() {

		return indexLocation;
	}

}
