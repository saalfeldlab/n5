package org.janelia.saalfeldlab.n5;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;
import org.janelia.saalfeldlab.n5.shard.Shards;

public class ShardedDatasetAttributes extends DatasetAttributes {

	private static final long serialVersionUID = -4559068841006651814L;

	private final int[] shardSize;

	private final IndexLocation indexLocation;

	public ShardedDatasetAttributes(final long[] dimensions, final int[] shardSize, final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		super(dimensions, getBlockSize(codecs), dataType, compression, codecs);
		final ShardingConfiguration config = getShardConfiguration(codecs);
		this.indexLocation = config.areIndexesAtStart() ? IndexLocation.start : IndexLocation.end;
		this.shardSize = shardSize;
	}

	public ShardedDatasetAttributes(final long[] dimensions,
			final int[] shardSize,
			final int[] blockSize,
			final IndexLocation shardIndexLocation,
			final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		super(dimensions, blockSize, dataType, compression, codecs);
		this.shardSize = shardSize;
		this.indexLocation = shardIndexLocation;
		// this.config = new ShardingConfiguration(blockSize, null, null, shardIndexLocation);

		// TODO figure out codecs
	}

	public int[] getShardSize() {

		return shardSize;
	}

	public Shards getShards() {

		return new Shards(this);
	}

	public ShardingConfiguration getShardingConfiguration() {

		return Arrays.stream(getCodecs())
				.filter(ShardingCodec::isShardingCodec)
				.map(x -> {
					return ((ShardingCodec)x).getConfiguration();
				})
				.findFirst().orElse(null);
	}

	public static boolean isSharded(Codec[] codecs) {

		return Arrays.stream(codecs).anyMatch(ShardingCodec::isShardingCodec);
	}

	public static ShardingConfiguration getShardConfiguration(Codec[] codecs) {

		return Arrays.stream(codecs)
				.filter(ShardingCodec::isShardingCodec)
				.map(x -> {
					return ((ShardingCodec)x).getConfiguration();
				})
				.findFirst().orElse(null);
	}

	public static int[] getBlockSize(Codec[] codecs) {

		return Arrays.stream(codecs)
				.filter(ShardingCodec::isShardingCodec)
				.map(x -> {
					return ((ShardingCodec)x).getConfiguration();
				})
				.map(ShardingConfiguration::getBlockSize).findFirst().orElse(null);
	}

}
