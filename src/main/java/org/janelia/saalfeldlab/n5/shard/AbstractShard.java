package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DatasetAttributes;


public abstract class AbstractShard<T> implements Shard<T> {

	protected DatasetAttributes datasetAttributes;

	protected ShardIndex index;

	private final int[] shardSize;

	private final int[] blockSize;

	private final long[] gridPosition;

	public <A extends DatasetAttributes & ShardParameters> AbstractShard(final A datasetAttributes, final long[] gridPosition,
			final ShardIndex index) {

		this.datasetAttributes = datasetAttributes;
		this.shardSize = datasetAttributes.getShardSize();
		this.blockSize = datasetAttributes.getBlockSize();
		this.gridPosition = gridPosition;
		this.index = index;
	}

	@Override
	public <A extends DatasetAttributes & ShardParameters> A getDatasetAttributes() {
		return (A)datasetAttributes;
	}

	@Override
	public int[] getSize() {

		return shardSize;
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
	public ShardIndex getIndex() {

		return index;
	}

}
