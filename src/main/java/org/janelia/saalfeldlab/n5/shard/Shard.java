package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.output.CountingOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.util.Position;

import static org.janelia.saalfeldlab.n5.N5Exception.*;

/**
 * Interface representing a shard - a container that holds multiple data blocks.
 * <p>
 * Blocks within a shard are addressed using positions relative to the shard's
 * internal grid, not the global dataset grid. Use {@link #getRelativeBlockPosition(long...)}
 * to convert between dataset-relative and shard-relative positions.
 * 
 * @param <T> the data type of blocks contained in this shard
 * 
 * @see ShardIndex
 * @see DatasetAttributes#getShardingCodec()
 */
public interface Shard<T> extends Iterable<DataBlock<T>> {

	/**
	 * Returns the number of blocks this shard contains along all dimensions.
	 * <p>
	 * The size of a shard is expected to be smaller than or equal to the spacing of the shard grid. 
	 * The dimensionality of size is expected to be equal to the dimensionality of the
	 * dataset. Consistency is not enforced.
	 *
	 * @return size of the shard in units of blocks
	 */
	default int[] getBlockGridSize() {

		return getDatasetAttributes().getBlocksPerShard();
	}

	/**
	 * Gets the dataset attributes associated with this shard.
	 * 
	 * @return the dataset attributes
	 */
	DatasetAttributes getDatasetAttributes();

	/**
	 * Returns the size of this shard in pixel units.
	 *
	 * @return shard size in pixels for each dimension
	 */
	default int[] getSize() {
		return getDatasetAttributes().getShardSize();
	}

	/**
	 * Returns the size of blocks in pixel units.
	 *
	 * @return block size in pixels for each dimension
	 */
	default int[] getBlockSize() {
		return getDatasetAttributes().getBlockSize();
	}

	/**
	 * Returns the position of this shard on the shard grid.
	 * <p>
	 * The dimensionality of the grid position is expected to be equal to the
	 * dimensionality of the dataset. Consistency is not enforced.
	 *
	 * @return position on the shard grid
	 */
	long[] getGridPosition();

	/**
	 * Converts a dataset-relative block position to a shard-relative block
	 * position.
	 * <p>
	 * Given a {@code blockPosition} relative to the dataset, return its
	 * position relative to this shard. This is necessary because blocks within
	 * a shard are indexed using their position within the shard's local grid,
	 * not their global dataset position.
	 *
	 * @param datasetBlockPosition
	 *            dataset-relative block position
	 * @return the shard-relative block position
	 * @see DatasetAttributes#getShardRelativeBlockPosition(long[], long[])
	 */
	default long[] getRelativeBlockPosition(long... datasetBlockPosition) {

		return getDatasetAttributes().getShardRelativeBlockPosition(
				getGridPosition(),
				datasetBlockPosition);
	}

	/**
	 * Tests whether the block at the given shard-relative position exists.
	 * <p>
	 * Avoids reading the block data by checking the shard index.
	 * {@link N5Writer#writeShard}
	 *
	 * @param relativeBlockPosition
	 *            the position relative to this shard
	 * @return true if the block exists in this shard, false otherwise
	 */
	default boolean blockExists(long... relativeBlockPosition) {

		return getIndex().exists(relativeBlockPosition);
	}

	/**
	 * Retrieves the DataBlock at the specified shard-relative position.
	 * <p>
	 * If needed, use {@link #getRelativeBlockPosition(long...)} to convert block positions 
	 * relative to the dataset into positions relative to this shard.
	 *
	 * @param blockGridPosition position of the block relative to this shard
	 * @return the block if it exists and is part of this shard, otherwise null
	 */
	DataBlock<T> getBlock(long... blockGridPosition);


	/**
	 * Removes a block from this shard.
	 * <p>
	 * Modifies this shard in-place such that writing this Shard with
	 * {N5Writer#w  
	 * 
	 * @param blockGridPosition position of the block relative to this shard
	 * @return true if the block existed and was removed, false otherwise
	 */
	boolean removeBlock(long... blockGridPosition);

	/**
	 * Returns an iterator over all blocks in this shard.
	 * <p>
	 * The iterator skips empty block positions and only returns blocks that
	 * actually exist in the shard.
	 * 
	 * @return an iterator over existing blocks
	 */
	default Iterator<DataBlock<T>> iterator() {

		return new DataBlockIterator<>(this);
	}

	/**
	 * Gets the total number of block positions in this shard.
	 * <p>
	 * Note that this returns the capacity (number of possible block positions),
	 * not the number of blocks that actually exist. Some positions may be empty.
	 * 
	 * @return the total number of block positions
	 */
	default int getNumBlocks() {

		return Arrays.stream(getBlockGridSize()).reduce(1, (x, y) -> x * y);
	}

	/**
	 * Gets a list of all blocks that exist in this shard.
	 * <p>
	 * This method materializes all blocks into a list, which may consume
	 * significant memory for large shards. Consider using the iterator
	 * instead for memory-efficient access.
	 *
	 * @return a list of all existing blocks
	 */
	default List<DataBlock<T>> getBlocks() {
		// TODO edit doc

		final List<DataBlock<T>> blocks = new ArrayList<>();
		for (DataBlock<T> block : this) {
			blocks.add(block);
		}
		return blocks;
	}

	/**
	 * Gets the {@link ShardIndex} for this shard.
	 * <p>
	 * If the shard doesn't exist yet, this may return an empty index.
	 *
	 * @return the ShardIndex for this shard
	 */
	ShardIndex getIndex();

	/**
	 * Creates an empty index of the correct size for this dataset.
	 *
	 * @return a new empty ShardIndex
	 */
	default ShardIndex createIndex() {

		final DatasetAttributes datasetAttributes = getDatasetAttributes();
		return datasetAttributes.getShardingCodec().createIndex(datasetAttributes);
	}

	/**
	 * Creates a ReadData representation of this shard's blocks and index.
	 * <p>
	 * This method serializes all blocks in the shard along with the shard index
	 * into a format suitable for storage. The index location (START or END) is
	 * determined by the sharding codec configuration.
	 * <p>
	 * The returned ReadData can be written to storage to persist the shard.
	 *
	 * @return a ReadData containing the serialized shard
	 * @throws N5IOException if serialization fails
	 */
	default ReadData createReadData() throws N5IOException {

		final DatasetAttributes datasetAttributes = getDatasetAttributes();
		ShardingCodec shardingCodec = datasetAttributes.getShardingCodec();
		final Codec.ArrayCodec arrayCodec = shardingCodec.getArrayCodec();

		final ShardIndex index = createIndex();
		long blocksStartBytes = index.getLocation() == ShardingCodec.IndexLocation.START ? index.numBytes() : 0;
		final AtomicLong blockOffset = new AtomicLong(blocksStartBytes);

		if (index.getLocation() == ShardingCodec.IndexLocation.END) {
			return ReadData.from(out -> {
				try (final CountingOutputStream countOut = new CountingOutputStream(out)) {
					long prevCount = 0;
					for (DataBlock<T> block : getBlocks()) {
						arrayCodec.encode(block).writeTo(countOut);
						final long[] blockPosition = getRelativeBlockPosition(block.getGridPosition());
						final long curCount = countOut.getByteCount();
						final long blockWrittenSize = curCount - prevCount;
						prevCount = curCount;
						synchronized (index) {
							index.set(blockOffset.getAndAdd(blockWrittenSize), blockWrittenSize, blockPosition);
						}
					}
					synchronized (index) {
						ShardIndex.write(out, index);
					}
				}
			});
		} else {
			final ArrayList<ReadData> blocksData = new ArrayList<>();
			for (DataBlock<T> dataBlock : getBlocks()) {
				ReadData readDataBlock = ReadData.from(out -> arrayCodec.encode(dataBlock).writeTo(out));
				blocksData.add(readDataBlock);
				final long length = readDataBlock.length();
				synchronized (index) {
					index.set(blockOffset.getAndAdd(length), length, getRelativeBlockPosition(dataBlock.getGridPosition()));
				}
			}
			return ReadData.from(out -> {
				ShardIndex.write(out, index);
				for (ReadData blockData : blocksData) {
					blockData.writeTo(out);
				}
			});
		}
	}

	/**
	 * Iterator implementation for traversing blocks within a shard.
	 * <p>
	 * This iterator only returns positions for blocks that exist, skipping
	 * empty positions in the shard grid.
	 *
	 * @param <T> the data type of blocks
	 */
	class DataBlockIterator<T> implements Iterator<DataBlock<T>> {

		private Shard<T> shard;
		private Iterator<Position> existingPositions;

		/**
		 * Creates a new iterator for the given shard.
		 *
		 * @param shard the shard to iterate over
		 */
		public DataBlockIterator(final Shard<T> shard) {

			this.shard = shard;
			existingPositions = shard.getIndex().iterator();
		}

		@Override
		public boolean hasNext() {
			return existingPositions.hasNext();
		}

		@Override
		public DataBlock<T> next() {

			return shard.getBlock(existingPositions.next().get());
		}
	}

}