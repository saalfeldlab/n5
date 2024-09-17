package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public interface Shard<T> {

	long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;

	/**
	 * Returns the number of blocks this shard contains along all dimensions.
	 *
	 * The size of a shard expected to be smaller than or equal to the spacing of the shard grid. The dimensionality of size is expected to be equal to the dimensionality of the
	 * dataset. Consistency is not enforced.
	 *
	 * @return size of the shard in units of blocks
	 */
	default int[] getBlockGridSize() {

		final int[] sz = getSize();
		final int[] blkSz = getBlockSize();
		final int[] blockGridSize = new int[sz.length];
		for (int i = 0; i < sz.length; i++)
			blockGridSize[i] = (int)(sz[i] / blkSz[i]);

		return blockGridSize;
	}

	public ShardedDatasetAttributes getDatasetAttributes();

	/**
	 * Returns the size of shards in pixel units.
	 *
	 * @return shard size
	 */
	public int[] getSize();

	/**
	 * Returns the size of blocks in pixel units.
	 *
	 * @return block size
	 */
	public int[] getBlockSize();

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
	default long[] getBlockPosition(long... blockPosition) {

		final long[] shardPos = getDatasetAttributes().getShardPositionForBlock(blockPosition);
		return getDatasetAttributes().getBlockPositionInShard(shardPos, blockPosition);
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

	public DataBlock<T> getBlock(long... position);

	public void writeBlock(DataBlock<T> block);

	public void writeShard();

	default DataBlock<T>[] getAllBlocks(long... position) {
		//TODO Caleb: Do we want this?
		return null;
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

	/**
	 * Say we want async datablock access
	 *
	 * Say we construct shard then getBlockAt
	 *
	 * (this could be how we do the aggregation) multiple getblockAt calls don't trigger reading read triggers reading of all blocks that were requested
	 *
	 * Shard doesn't hold the data directly, but is the metadata about how the blocks are stored
	 *
	 */
}
