package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

public class InMemoryShard<T> extends AbstractShard<T> {

	private List<DataBlock<T>> blocks;

	private ShardIndex shardIndex;

	public InMemoryShard(final long[] size, final long[] gridPosition, final int[] blockSize, T type) {

		super(size, gridPosition, blockSize, type);
		blocks = new ArrayList<>();
	}

	@Override
	public DataBlock<T> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) {

		// TODO Auto-generated method stub
		return null;
	}

}
