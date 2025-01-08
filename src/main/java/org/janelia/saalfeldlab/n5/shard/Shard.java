package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public interface Shard<T> extends Iterable<DataBlock<T>> {

	long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;

	public ShardedDatasetAttributes getDatasetAttributes();

	/**
	 * Returns the number of blocks this shard contains along all dimensions.
	 *
	 * The size of a shard expected to be smaller than or equal to the spacing of the shard grid. The dimensionality of size is expected to be equal to the dimensionality of the
	 * dataset. Consistency is not enforced.
	 *
	 * @return size of the shard in units of blocks
	 */
	default int[] getBlockGridSize() {

		return getDatasetAttributes().getBlocksPerShard();
	}

	/**
	 * Returns the size of shards in pixel units.
	 *
	 * @return shard size
	 */
	default int[] getSize() {
		return getDatasetAttributes().getShardSize();
	}

	/**
	 * Returns the size of blocks in pixel units.
	 *
	 * @return block size
	 */
	default int[] getBlockSize() {
		return getDatasetAttributes().getBlockSize();
	}

	/**
	 * Returns the position of this shard on the shard grid.
	 *
	 * The dimensionality of the grid position is expected to be equal to the dimensionality of the dataset. Consistency is not enforced.
	 *
	 * @return position on the shard grid
	 */
	public long[] getGridPosition();

	/**
	 * Returns of the block at the given position relative to this shard, or null if this shard does not contain the given block.
	 *
	 * @return the shard position
	 */
	default int[] getBlockPosition(long... blockPosition) {

		final long[] shardPos = getDatasetAttributes().getShardPositionForBlock(blockPosition);
		return getDatasetAttributes().getBlockPositionInShard(shardPos, blockPosition);
	}
	
	/**
	 * Returns the position in pixels of the 
	 * 
	 * @return the min 
	 */
	default long[] getShardMinPosition(long... shardPosition) {

		final int[] shardSize = getSize();
		final long[] shardMin = new long[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			shardMin[i] = shardPosition[i] * shardSize[i];
		}
		return shardMin;
	}

	/**
	 * Returns the position of the shard containing the block with the given block position.
	 *
	 * @return the shard position
	 */
	default long[] getShard(long... blockPosition) {

		final int[] shardBlockDimensions = getBlockGridSize();
		final long[] shardGridPosition = new long[shardBlockDimensions.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (long)Math.floor((double)(blockPosition[i]) / shardBlockDimensions[i]);
		}

		return shardGridPosition;
	}

	public DataBlock<T> getBlock(long... blockGridPosition);

	public void writeBlock(DataBlock<T> block);

	default Iterator<DataBlock<T>> iterator() {

		return new DataBlockIterator<>(this);
	}

	default List<DataBlock<T>> getBlocks() {

		final List<DataBlock<T>> blocks = new ArrayList<>();
		for (DataBlock<T> block : this) {
			if (block != null)
				blocks.add(block);
		}
		return blocks;
	}

	public ShardIndex getIndex();

	public static <T> Shard<T> createEmpty(final ShardedDatasetAttributes attributes, long... shardPosition) {

		final long[] emptyIndex = new long[(int)(2 * attributes.getNumBlocks())];
		Arrays.fill(emptyIndex, EMPTY_INDEX_NBYTES);
		final ShardIndex shardIndex = new ShardIndex(attributes.getBlocksPerShard(), emptyIndex, ShardingCodec.IndexLocation.END);
		return new InMemoryShard<T>(attributes, shardPosition, shardIndex);
	}

	public static long flatIndex(long[] gridPosition, int[] gridSize) {

		long index = gridPosition[0];
		long cumSizes = gridSize[0];
		for (int i = 1; i < gridSize.length; i++) {
			index += gridPosition[i] * cumSizes;
			cumSizes *= gridSize[i];
		}
		return index;
	}

	public static class DataBlockIterator<T> implements Iterator<DataBlock<T>> {

		private final GridIterator it;
		private final Shard<T> shard;

		public DataBlockIterator(final Shard<T> shard) {

			this.shard = shard;
			it = new GridIterator(shard.getBlockGridSize());
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public DataBlock<T> next() {
			return shard.getBlock(it.next());
		}
	}
}
