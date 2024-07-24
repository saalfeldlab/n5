package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public class InMemoryShard<T> extends AbstractShard<T> {

	private List<DataBlock<T>> blocks;

	public InMemoryShard(final ShardedDatasetAttributes datasetAttributes, final long[] gridPosition,
			ShardIndex index) {

		super(datasetAttributes, gridPosition, index);
		blocks = new ArrayList<>();
	}

}
