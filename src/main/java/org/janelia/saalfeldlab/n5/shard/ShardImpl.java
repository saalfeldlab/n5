package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

public class ShardImpl<T> extends AbstractShard<T> {

	public ShardImpl(final long[] size, final long[] gridPosition, final int[] blockSize, T type) {

		super(size, gridPosition, blockSize, type);
	}

	@Override
	public DataBlock<T> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) {

		// TODO Auto-generated method stub
		return null;
	}

}
