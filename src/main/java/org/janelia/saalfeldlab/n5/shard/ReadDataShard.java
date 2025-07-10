package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class ReadDataShard<T> extends VirtualShard<T> {

	private ReadData blockReadData; 

	public ReadDataShard(final DatasetAttributes datasetAttributes, final long[] gridPosition, final ReadData blockData) {

		super(datasetAttributes, gridPosition, blockData.materialize());
		this.blockReadData = super.createReadData();
	}

	private ReadDataShard(final DatasetAttributes datasetAttributes, long[] gridPosition, final ReadData totalShardData, final ShardIndex index) {
		this(datasetAttributes, gridPosition, blockReadData(totalShardData, index));
		super.index = index;
	}

	private static ReadData blockReadData(final ReadData totalShardData, final ShardIndex index) {

		if (index.getLocation() == ShardingCodec.IndexLocation.START)
			return totalShardData.slice(index.numBytes(), -1);
		else {
			return totalShardData.slice(0, totalShardData.length() - index.numBytes());
		}
	}

	@Override
	public boolean removeBlock(long... blockGridPosition) {

		if( !index.exists(blockGridPosition))
			return false;

		final long blkOffset = index.getOffset(blockGridPosition);
		final long blkLength = index.getNumBytes(blockGridPosition);

		final byte[] newData = new byte[(int)(blockReadData.length() - blkLength)];
		final byte[] oldData = blockReadData.allBytes();

		// Copy data before the removed block
		if (blkOffset > 0) {
			System.arraycopy(oldData, 0, newData, 0, (int)blkOffset);
		}
		
		// Copy data after the removed block
		final long afterBlockOffset = blkOffset + blkLength;
		if (afterBlockOffset < oldData.length) {
			System.arraycopy(oldData, (int)afterBlockOffset, newData, (int)blkOffset, 
					(int)(oldData.length - afterBlockOffset));
		}

		blockReadData = ReadData.from(newData);

		// set the index for this position to empty
		index.setEmpty(blockGridPosition);

		// Update offsets in index for all blocks that come after the removed block
		updateOffsetsAfterRemoval(blkOffset, blkLength);
		
		return true;
	}

	/**
	 * Updates the offsets in the index for all blocks that come after the removed block.
	 * 
	 * @param removedOffset the offset of the removed block
	 * @param removedLength the length of the removed block
	 */
	private void updateOffsetsAfterRemoval(long removedOffset, long removedLength) {
		for (int flatIndex = 0; flatIndex < index.getBlockCapacity(); flatIndex++) {
			final long thisOffset = index.getOffsetByBlockIndex(flatIndex);
			if (thisOffset != ShardIndex.EMPTY_INDEX_NBYTES && thisOffset > removedOffset)
				index.setOffsetAtIndex(index.getOffsetByBlockIndex(flatIndex) - removedLength, flatIndex);
		}
	}

	private ReadData getEncodedBlock(long... relativePosition) {

		final ShardIndex idx = getIndex();
		if (!idx.exists(relativePosition))
			return null;

		final long blockOffset = idx.getOffset(relativePosition);
		final long blockSize = idx.getNumBytes(relativePosition);

		final ReadData blockData = blockReadData.slice(blockOffset, blockSize);
		return blockData;
	}

	@Override
	public DataBlock<T> getBlock(long... relativePosition) {

		final long[] blockPosInDataset = getDatasetAttributes().getBlockPositionFromShardPosition(getGridPosition(), relativePosition);
		try {
			final ReadData encodedBlock = getEncodedBlock(relativePosition);
			if (encodedBlock == null)
				return null;

			return getBlock(encodedBlock, blockPosInDataset);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + Arrays.toString(blockPosInDataset), e);
		}
	}

	@Override
	public ReadData createReadData() {

		if (index.getLocation() == ShardingCodec.IndexLocation.END) {
			return ReadData.from(out -> {
				blockReadData.writeTo(out);
				ShardIndex.write(out, index);
			});
		} else {
			return ReadData.from(out -> {
				ShardIndex.write(out, index);
				blockReadData.writeTo(out);
			});
		}
	}

	public static <T> ReadDataShard<T> fromShard(Shard<T> shard) {

		if (shard instanceof ReadDataShard)
			return (ReadDataShard<T>)shard;

		final ReadDataShard<T> readDataShard = new ReadDataShard<T>(
				shard.getDatasetAttributes(),
				shard.getGridPosition(),
				shard.createReadData(),
				shard.getIndex());

		return readDataShard;
	}

}
