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
	default int[] getBlockPosition(long... blockPosition) {

		final long[] shardPos = getShard(blockPosition);
		if (!Arrays.equals(getGridPosition(), shardPos))
			return null;

		final int[] shardSize = getSize();
		final int[] blkSize = getBlockSize();
		final int[] blkGridSize = getBlockGridSize();

		final int[] blockShardPos = new int[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			final long shardP = shardPos[i] * shardSize[i];
			final long blockP = blockPosition[i] * blkSize[i];
			blockShardPos[i] = (int)((blockP - shardP) / blkGridSize[i]);
		}

		return blockShardPos;
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
			shardGridPosition[i] = (long)Math.floor((double)blockPosition[i] / shardBlockDimensions[i]);
		}

		return shardGridPosition;
	}

	public DataBlock<T> getBlock(int... position);

	default DataBlock<T>[] getAllBlocks(int... position) {
		//TODO Caleb: Do we want this?
		return null;
	}

	public ShardIndex getIndex();

	public static <T> Shard<T> createEmpty(final ShardedDatasetAttributes attributes, long... shardPosition) {

		final long[] emptyIndex = new long[(int)(2 * attributes.getNumBlocks())];
		Arrays.fill(emptyIndex, EMPTY_INDEX_NBYTES);
		final ShardIndex shardIndex = new ShardIndex(attributes.getShardBlockGridSize(), emptyIndex);

		return new InMemoryShard<T>(
				attributes.getShardSize(),
				shardPosition,
				attributes.getBlockSize(),
				shardIndex);
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
