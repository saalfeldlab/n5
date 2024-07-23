package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

public class InMemoryShard<T> extends AbstractShard<T> {

	private List<DataBlock<T>> blocks;

	private ShardIndex shardIndex;

	public InMemoryShard(final int[] shardSize, final long[] gridPosition, final int[] blockSize, ShardIndex index) {

		super(shardSize, gridPosition, blockSize, index);
		blocks = new ArrayList<>();
	}

}
