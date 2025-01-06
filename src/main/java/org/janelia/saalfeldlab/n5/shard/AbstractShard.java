package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DatasetAttributes;

public abstract class AbstractShard<T, A extends DatasetAttributes & ShardParameters> implements Shard<T,A> {

	protected final A datasetAttributes;

	protected ShardIndex index;

	private final long[] gridPosition;

	public AbstractShard(final A datasetAttributes, final long[] gridPosition,
			final ShardIndex index) {

		this.datasetAttributes = datasetAttributes;
		this.gridPosition = gridPosition;
		this.index = index;
	}

	@Override
	public A getDatasetAttributes() {

		return datasetAttributes;
	}

	@Override
	public int[] getSize() {

		return datasetAttributes.getShardSize();
	}

	@Override
	public int[] getBlockSize() {

		return datasetAttributes.getBlockSize();
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
