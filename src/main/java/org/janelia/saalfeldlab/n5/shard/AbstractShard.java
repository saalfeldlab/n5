package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;

public abstract class AbstractShard<T> implements Shard<T> {

	protected final long[] size;
	protected final long[] gridPosition;
	protected final int[] blockSize;

	public AbstractShard(final long[] size, final long[] gridPosition,
			final int[] blockSize, final T type) {

		this.size = size;
		this.gridPosition = gridPosition;
		this.blockSize = blockSize;
	}

	@Override
	public long[] getSize() {

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
	public ShardIndex getIndexes() {

		return null;
	}


}
