package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.DataBlock;

/**
 * Wrap a RawShard as a DataBlock.
 * This basically just adds a gridPosition for the shard.
 */
public class RawShardDataBlock implements DataBlock<RawShard> {

	private final long[] gridPosition;

	private final RawShard shard;

	RawShardDataBlock(final long[] gridPosition, final RawShard shard) {
		this.gridPosition = gridPosition;
		this.shard = shard;
	}

	@Override
	public int[] getSize() {
		return shard.index().size();
	}

	@Override
	public long[] getGridPosition() {
		return gridPosition;
	}

	@Override
	public int getNumElements() {
		return shard.index().numElements();
	}

	@Override
	public RawShard getData() {
		return shard;
	}
}
