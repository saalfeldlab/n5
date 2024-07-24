package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public abstract class AbstractShard<T> implements Shard<T> {

	protected final ShardedDatasetAttributes datasetAttributes;

	protected final ShardIndex index;

	private final long[] gridPosition;

	public AbstractShard(final ShardedDatasetAttributes datasetAttributes, final long[] gridPosition,
			final ShardIndex index) {

		this.datasetAttributes = datasetAttributes;
		this.gridPosition = gridPosition;
		this.index = index;
	}

	public ShardedDatasetAttributes getDatasetAttributes() {

		return datasetAttributes;
	}

	@Override
	public int[] getSize() {

		return datasetAttributes.getShardSize();
	}

	@Override
	public int[] getBlockSize() {

		return datasetAttributes.getShardSize();
	}

	@Override
	public long[] getGridPosition() {

		return gridPosition;
	}

	@Override
	public DataBlock<T> getBlock(long... position) {

		return null;
	}

	@Override
	public ShardIndex getIndex() {

		return index;
	}


}
