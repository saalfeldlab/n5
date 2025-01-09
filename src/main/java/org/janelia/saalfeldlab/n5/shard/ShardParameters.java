package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.BlockParameters;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public interface ShardParameters extends BlockParameters {

	public ShardingCodec getShardingCodec();

	/**
	 * The size of the blocks in pixel units.
	 *
	 * @return the number of pixels per dimension for this shard.
	 */
	public int[] getShardSize();

	public IndexLocation getIndexLocation();

	default ShardIndex createIndex() {
		return new ShardIndex(getBlocksPerShard(), getIndexLocation(), getShardingCodec().getIndexCodecs());
	}

	/**
	 * Returns the number of blocks per dimension for a shard.
	 *
	 * @return the size of the block grid of a shard
	 */
	default int[] getBlocksPerShard() {

		final int nd = getNumDimensions();
		final int[] blocksPerShard = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			blocksPerShard[i] = getShardSize()[i] / blockSize[i];

		return blocksPerShard;
	}
	
	/**
	 * Given a block's position relative to the array, returns the position of the shard containing that block relative to the shard grid.
	 *
	 * @param blockGridPosition
	 *            position of a block relative to the array
	 * @return the position of the containing shard in the shard grid
	 */
	default long[] getShardPositionForBlock(final long... blockGridPosition) {

		final int[] blocksPerShard = getBlocksPerShard();
		final long[] shardGridPosition = new long[blockGridPosition.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (int)Math.floor((double)blockGridPosition[i] / blocksPerShard[i]);
		}

		return shardGridPosition;
	}
	
	/**
	 * Returns the number of shards per dimension for the dataset.
	 *
	 * @return the size of the shard grid of a dataset
	 */
	default int[] getShardBlockGridSize() {

		final int nd = getNumDimensions();
		final int[] shardBlockGridSize = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			shardBlockGridSize[i] = (int)(Math.ceil((double)getDimensions()[i] / blockSize[i]));

		return shardBlockGridSize;
	}

	/**
	 * Returns the block at the given position relative to this shard, or null if this shard does not contain the given block.
	 *
	 * @return the block position
	 */
	default int[] getBlockPositionInShard(final long[] shardPosition, final long[] blockPosition) {

		// TODO check correctness 
		final long[] shardPos = getShardPositionForBlock(blockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] shardSize = getBlocksPerShard();
		final int[] blockShardPos = new int[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			blockShardPos[i] = (int)(blockPosition[i] % shardSize[i]);
		}

		return blockShardPos;
	}
	

	/**
	 * Given a block's position relative to a shard, returns its position in pixels
	 * relative to the image.
	 * 
	 * @param shardPosition shard position in the shard grid
	 * @param blockPosition block position the 
	 * @return the block's min pixel coordinate
	 */
	default long[] getBlockMinFromShardPosition(final long[] shardPosition, final long[] blockPosition) {

		// is this useful?
		final int[] blockSize = getBlockSize();
		final int[] shardSize = getShardSize();
		final long[] blockImagePos = new long[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			blockImagePos[i] = (shardPosition[i] * shardSize[i]) + (blockPosition[i] * blockSize[i]);
		}

		return blockImagePos;
	}

	/**
	 * Given a block's position relative to a shard, returns its position relative
	 * to the image.
	 *
	 * @param shardPosition shard position in the shard grid
	 * @param blockPosition block position relative to the shard 
	 * @return the block position in the block grid
	 */
	default long[] getBlockPositionFromShardPosition(final long[] shardPosition, final long[] blockPosition) {

		// is this useful?
		final int[] shardBlockSize = getBlocksPerShard();
		final long[] blockImagePos = new long[getNumDimensions()];
		for (int i = 0; i < getNumDimensions(); i++) {
			blockImagePos[i] = (shardPosition[i] * shardBlockSize[i]) + (blockPosition[i]);
		}

		return blockImagePos;
	}
	
	/**
	 * @return the number of blocks per shard
	 */
	default long getNumBlocks() {

		return Arrays.stream(getBlocksPerShard()).reduce(1, (x, y) -> x * y);
	}

}
