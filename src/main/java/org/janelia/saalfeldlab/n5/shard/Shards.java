package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

/**
 * Manages the set of shards that comprise a dataset.
 */
public class Shards {

	public static long EMPTY_INDEX_NBYTES = 0xFFFFFFFFFFFFFFFFL;

	private final ShardedDatasetAttributes datasetAttributes;

	public Shards(final ShardedDatasetAttributes datasetAttributes) {

		this.datasetAttributes = datasetAttributes;
	}

	public ShardedDatasetAttributes getDatasetAttributes() {

		return datasetAttributes;
	}

	public int[] getShardSize() {

		return getDatasetAttributes().getShardSize();
	}

	public int[] getBlockSize() {

		return getDatasetAttributes().getBlockSize();
	}

	/**
	 * Returns the number of blocks a shard contains along all dimensions.
	 *
	 * @return the size of the block grid of a shard
	 */
	public int[] getShardBlockGridSize() {

		final int nd = getDatasetAttributes().getNumDimensions();
		final int[] shardBlockGridSize = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			shardBlockGridSize[i] = (int)(Math
					.ceil((double)getDatasetAttributes().getDimensions()[i] / blockSize[i]));

		return shardBlockGridSize;
	}

	/**
	 * Given a block's position relative to the array, returns the position of the shard containing that block relative to the shard grid.
	 *
	 * @param gridPosition
	 *            position of a block relative to the array
	 * @return the position of the containing shard in the shard grid
	 */
	public long[] getShardPositionForBlock(final long... blockGridPosition) {

		// TODO have this return a shard
		final int[] shardBlockDimensions = getShardBlockGridSize();
		final long[] shardGridPosition = new long[blockGridPosition.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (int)Math.floor((double)blockGridPosition[i] / shardBlockDimensions[i]);
		}

		return shardGridPosition;
	}

	/**
	 * Returns of the block at the given position relative to this shard, or null if this shard does not contain the given block.
	 *
	 * @return the shard position
	 */
	public int[] getBlockPositionInShard(final long[] shardPosition, final long[] blockPosition) {

		final long[] shardPos = getShardPositionForBlock(blockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] shardSize = getShardSize();
		final int[] blkSize = getBlockSize();
		final int[] blkGridSize = getShardBlockGridSize();

		final int[] blockShardPos = new int[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			final long shardP = shardPos[i] * shardSize[i];
			final long blockP = blockPosition[i] * blkSize[i];
			blockShardPos[i] = (int)((blockP - shardP) / blkGridSize[i]);
		}

		return blockShardPos;
	}

	/**
	 * @return the number of blocks per shard
	 */
	public long getNumBlocks() {

		return Arrays.stream(getShardBlockGridSize()).reduce(1, (x, y) -> x * y);
	}


}
