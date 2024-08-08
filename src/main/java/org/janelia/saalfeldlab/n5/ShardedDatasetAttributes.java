package org.janelia.saalfeldlab.n5;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.ByteStreamCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

public class ShardedDatasetAttributes extends DatasetAttributes {

	private static final long serialVersionUID = -4559068841006651814L;

	private final int[] shardSize;

	private final IndexLocation indexLocation;

	public ShardedDatasetAttributes(
			final long[] dimensions,
			final int[] shardSize,
			final int[] blockSize,
			final IndexLocation shardIndexLocation,
			final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		super(dimensions, blockSize, dataType, compression, codecs);
		this.shardSize = shardSize;
		this.indexLocation = shardIndexLocation;

		// TODO figure out codecs
	}

	public int[] getShardSize() {

		return shardSize;
	}

	/**
	 * Returns the number of blocks a shard contains along all dimensions.
	 *
	 * @return the size of the block grid of a shard
	 */
	public int[] getShardBlockGridSize() {

		final int nd = getNumDimensions();
		final int[] shardBlockGridSize = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			shardBlockGridSize[i] = (int)(Math.ceil((double)getDimensions()[i] / blockSize[i]));

		return shardBlockGridSize;
	}

	/**
	 * Given a block's position relative to the array, returns the position of the shard containing that block relative to the shard grid.
	 *
	 * @param blockGridPosition
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

	public static int[] getBlockSize(ByteStreamCodec[] codecs) {

		//TODO Caleb: Move this?
		return Arrays.stream(codecs)
				.filter(ShardingCodec::isShardingCodec)
				.map(x -> ((ShardingCodec)x).getBlockSize())
				.findFirst().orElse(null);
	}

	public IndexLocation getIndexLocation() {

		return indexLocation;
	}
}
