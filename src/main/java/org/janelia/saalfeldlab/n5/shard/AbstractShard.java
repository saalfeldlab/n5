package org.janelia.saalfeldlab.n5.shard;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;


public abstract class AbstractShard<T> implements Shard<T> {

	protected final DatasetAttributes datasetAttributes;

	protected ShardIndex index;

	private final long[] gridPosition;

	private final int[] size;		// in pixels

	private final int[] blockSize;	// in pixels

	public <A extends DatasetAttributes & ShardParameters> AbstractShard(final A datasetAttributes, final long[] gridPosition,
			final ShardIndex index) {

		this.datasetAttributes = datasetAttributes;
		this.gridPosition = gridPosition;
		this.index = index;

		this.size = datasetAttributes.getShardSize();
		this.blockSize = datasetAttributes.getBlockSize();
	}

	@Override
	public <A extends DatasetAttributes & ShardParameters> A getDatasetAttributes() {

		return (A)datasetAttributes;
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
	public ShardIndex getIndex() {

		return index;
	}
	
	public DataBlock<T> getBlock(long... blockGridPosition) {

		return getChildBlock(getShardPositionForBlock(blockGridPosition))
				.getBlock(blockGridPosition);
	}
	
	private long[] getShardPositionForBlock(final long... blockGridPosition) {

		final long[] shardGridPosition = new long[blockGridPosition.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (int)Math.floor((double)blockGridPosition[i] * blockSize[i]/ size[i]);
		}

		return shardGridPosition;
	}

	@Override
	public void readData(ByteBuffer buffer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readData(DataInput inputStream) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeData(DataOutput output) throws IOException {

		if (getDatasetAttributes().getIndexLocation() == IndexLocation.START)
			index.writeData(output);

		for (DataBlock<T> block : getBlocks())
			block.writeData(output);

		if (getDatasetAttributes().getIndexLocation() == IndexLocation.END)
			index.writeData(output);
	}

}
