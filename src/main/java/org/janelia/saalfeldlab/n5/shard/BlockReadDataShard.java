package org.janelia.saalfeldlab.n5.shard;

import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.util.FinalPosition;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;

public class BlockReadDataShard<T> extends AbstractShard<T> {

	private final Map<Position, ReadData> blocks;
	
	public BlockReadDataShard(final DatasetAttributes datasetAttributes, final long[] gridPosition) {
		this(datasetAttributes, gridPosition, null, datasetAttributes.getShardingCodec().createIndex(datasetAttributes));
	}

	public BlockReadDataShard(final DatasetAttributes datasetAttributes, final long[] gridPosition, final ReadData totalShardData) {
		this(datasetAttributes, gridPosition, totalShardData, 
				readShardIndex(datasetAttributes, totalShardData));
	}

	private BlockReadDataShard(
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition, 
			final ReadData totalShardData,
			final ShardIndex index) {

		super(datasetAttributes, gridPosition, index);
		this.blocks = blockReadData(
				totalShardData, 
				datasetAttributes.getBlocksPerShard(),
				getIndex());
	}
	
	private static ShardIndex readShardIndex(
			final DatasetAttributes datasetAttributes,
			final ReadData totalShardData)
	{
		final ShardIndex index = datasetAttributes.getShardingCodec().createIndex(datasetAttributes);
		ShardIndex.read(totalShardData, index);
		return index;
	}

	private static Map<Position,ReadData> blockReadData(final ReadData totalShardData, final int[] blocksPerShard, final ShardIndex index) {

		final Map<Position, ReadData> blockReadData = new TreeMap<>();
		if (totalShardData == null)
			return blockReadData;

		final GridIterator it = new GridIterator( blocksPerShard );
		while( it.hasNext()) {

			final long[] p = it.next();
			if( index.exists(p) )
			{
				blockReadData.put(new FinalPosition(p.clone()),
						totalShardData.slice(index.getOffset(p), index.getNumBytes(p)));
			}
		}
		return blockReadData;
	}
	
	/**
	 * Add the {@code block} to this shard. If the block is not contained in this shard, do not add it.
	 *
	 * @param block to add the shard
	 * @return whether the block was added
	 */
	public boolean addBlock(final DataBlock<T> block) {
		
		final long[] blockPos = block.getGridPosition();
		final long[] shardPositionForBlock = datasetAttributes.getShardPositionForBlock(blockPos);
		if (!Arrays.equals(shardPositionForBlock, getGridPosition()))
			return false;

		final long[] relativeBlockPos = getRelativeBlockPosition(blockPos);

		// remove a block if it exists to update the index properly
		removeBlock(relativeBlockPos);

		final ReadData blockData = datasetAttributes.getArrayCodec().encode(block).materialize();
		blocks.put(Position.wrap(relativeBlockPos), blockData);

		// update the index with the newly added block
		final long newOffset = index.numBlockDataBytes();
		index.set(newOffset, blockData.length(), relativeBlockPos);

		return true;
	}

	@Override
	public boolean removeBlock(long... blockGridPosition) {

		if( !index.exists(blockGridPosition))
			return false;

		final long blkOffset = index.getOffset(blockGridPosition);
		final long blkLength = index.getNumBytes(blockGridPosition);
		blocks.remove(Position.wrap(blockGridPosition));

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

		return blocks.get(Position.wrap(relativePosition));
	}

	@Override
	public DataBlock<T> getBlock(long... relativePosition) {

		final long[] blockPosInDataset = getDatasetAttributes().getBlockPositionFromShardPosition(getGridPosition(), relativePosition);
		try {
			final ReadData encodedBlock = getEncodedBlock(relativePosition);
			if (encodedBlock == null)
				return null;

			return datasetAttributes.getShardingCodec().getArrayCodec().decode(encodedBlock, blockPosInDataset);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final N5IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + Arrays.toString(blockPosInDataset), e);
		}
	}

	@Override
	public ReadData createReadData() {

		if (index.getLocation() == ShardingCodec.IndexLocation.END) {
			return ReadData.from(out -> {
				writeBlocksInIndexOrder(out);
				ShardIndex.write(out, index);
			});
		} else {
			return ReadData.from(out -> {
				ShardIndex.write(out, index);
				writeBlocksInIndexOrder(out);
			});
		}
	}

	private void writeBlocksInIndexOrder(final OutputStream out) {

		final Iterator<Position> it = index.storageIterator();
		while (it.hasNext()) {
			blocks.get(it.next()).writeTo(out);
		}
	}

	public static <T> BlockReadDataShard<T> fromShard(Shard<T> shard) {

		if (shard instanceof BlockReadDataShard)
			return (BlockReadDataShard<T>)shard;

		final ReadData shardData = shard.createReadData();
		final BlockReadDataShard<T> readDataShard = new BlockReadDataShard<T>(
				shard.getDatasetAttributes(),
				shard.getGridPosition(),
				shardData == null ? null : shardData.materialize(),
				shard.getIndex());

		return readDataShard;
	}

}
