package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;

public abstract class AbstractShard<T> implements Shard<T> {

	protected final int[] size;
	protected final long[] gridPosition;
	protected final int[] blockSize;
	private final ShardIndex index;

	public AbstractShard(final int[] shardSize, final long[] gridPosition,
			final int[] blockSize, final ShardIndex index) {

		this.size = shardSize;
		this.gridPosition = gridPosition;
		this.blockSize = blockSize;
		this.index = index;
	}

	@Override
	public int[] getSize() {

		return size;
	}

	@Override
	public int[] getBlockSize() {

		return blockSize;
	}

	@Override
	public long[] getGridPosition() {

		return gridPosition;
	}

	@Override
	public DataBlock<T> getBlock(int... position) {

		return null;
	}

	@Override
	public ShardIndex getIndex() {

		return index;
	}


}
