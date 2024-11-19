package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public class ShardIndexBuilder {

	private final Shard<?> shard;

	private final ShardIndex temporaryIndex;

	private IndexLocation location = IndexLocation.END;

	private DeterministicSizeCodec[] codecs;
	
	private long currentOffset = 0;

	public ShardIndexBuilder(Shard<?> shard) {

		this.shard = shard;
		this.temporaryIndex = new ShardIndex(shard.getBlockGridSize(), location);
	}

	public ShardIndex build() {

		return new ShardIndex(
				shard.getBlockGridSize(),
				temporaryIndex.getData(),
				location,
				codecs);
	}

	public ShardIndexBuilder indexLocation(IndexLocation location) {

		this.location = location;
		return this;
	}

	public ShardIndexBuilder setCodecs(DeterministicSizeCodec... codecs) {

		this.codecs = codecs;
		return this;
	}

	public ShardIndexBuilder addBLock(long[] blockPosition, long numBytes) {

		final long[] blockPositionInShard = shard.getDatasetAttributes().getBlockPositionInShard(
				shard.getGridPosition(),
				blockPosition);

		if (blockPositionInShard == null) {
			throw new IllegalArgumentException(String.format(
					"The block at position %s is not contained in the shard at position : %s and size : %s )",
					Arrays.toString(blockPosition),
					Arrays.toString(shard.getGridPosition()),
					Arrays.toString(shard.getSize())));
		}

		temporaryIndex.set(currentOffset, numBytes, blockPositionInShard);
		currentOffset += numBytes;

		return this;
	}

}
