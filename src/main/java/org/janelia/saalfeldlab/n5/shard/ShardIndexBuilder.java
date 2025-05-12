package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public class ShardIndexBuilder {

	private final Shard<?> shard;

	private ShardIndex temporaryIndex;

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
		this.temporaryIndex = new ShardIndex(shard.getBlockGridSize(), location);
		updateInitialOffset();
		return this;
	}

	public IndexLocation getLocation() {

		return this.location;
	}

	public ShardIndexBuilder setCodecs(DeterministicSizeCodec... codecs) {

		this.codecs = codecs;
		final ShardIndex newIndex = new ShardIndex(shard.getBlockGridSize(), temporaryIndex.getLocation(), codecs);
		this.temporaryIndex = newIndex;
		updateInitialOffset();
		return this;
	}

	public ShardIndexBuilder addBlock(long[] blockPosition, long numBytes) {
		//TODO Caleb: Maybe move to ShardIndex?
		final int[] blockPositionInShard = shard.getDatasetAttributes().getBlockPositionInShard(
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

	private void updateInitialOffset() {

		if (location == IndexLocation.END)
			currentOffset = 0;
		else
			currentOffset = temporaryIndex.numBytes();

	}

}
