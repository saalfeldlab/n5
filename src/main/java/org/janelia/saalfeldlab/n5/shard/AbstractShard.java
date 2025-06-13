package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DatasetAttributes;


public abstract class AbstractShard<T> implements Shard<T> {

	protected final DatasetAttributes datasetAttributes;

	protected ShardIndex index;

	private final long[] gridPosition;

	public AbstractShard(
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition,
			final ShardIndex index) {

		this.datasetAttributes = datasetAttributes;
		this.gridPosition = gridPosition;
		this.index = index;
	}

	@Override
	public DatasetAttributes getDatasetAttributes() {

		return datasetAttributes;
	}

	@Override
	public int[] getSize() {

		return getDatasetAttributes().getShardSize();
	}

	@Override
	public int[] getBlockSize() {

		return datasetAttributes.getBlockSize();
	}

	@Override
	public long[] getGridPosition() {

		return gridPosition;
	}
}
