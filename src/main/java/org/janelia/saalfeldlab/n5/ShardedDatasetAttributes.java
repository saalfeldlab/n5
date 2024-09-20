package org.janelia.saalfeldlab.n5;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardIndex;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public class ShardedDatasetAttributes extends DatasetAttributes {

	private static final long serialVersionUID = -4559068841006651814L;

	private final int[] shardSize;

	private final ShardingCodec shardingCodec;

	public ShardedDatasetAttributes (
			final long[] dimensions,
			final int[] shardSize, //in pixels
			final int[] blockSize, //in pixels
			final DataType dataType,
			final Codec[] blocksCodecs,
			final DeterministicSizeCodec[] indexCodecs,
			final IndexLocation indexLocation
	) {
		super(dimensions, blockSize, dataType, null, blocksCodecs);
		this.shardSize = shardSize;
		this.shardingCodec = new ShardingCodec(
				blockSize,
				blocksCodecs,
				indexCodecs,
				indexLocation
		);
	}

	public ShardedDatasetAttributes(
			final long[] dimensions,
			final int[] shardSize, //in pixels
			final int[] blockSize, //in pixels
			final DataType dataType,
			final ShardingCodec codec) {
		super(dimensions, blockSize, dataType, null, null);
		this.shardSize = shardSize;
		this.shardingCodec = codec;
	}

	public ShardingCodec getShardingCodec() {
		return shardingCodec;
	}

	@Override public ArrayCodec getArrayCodec() {

		return shardingCodec.getArrayCodec();
	}

	@Override public BytesCodec[] getCodecs() {

		return shardingCodec.getCodecs();
	}

	public int[] getShardSize() {

		return shardSize;
	}

	/**
	 * Returns the number of shards per dimension for the dataset.
	 *
	 * @return the size of the shard grid of a dataset
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
	 * Returns the number of blocks per dimension for a shard.
	 *
	 * @return the size of the block grid of a shard
	 */
	public int[] getBlocksPerShard() {

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
	public long[] getShardPositionForBlock(final long... blockGridPosition) {

		// TODO have this return a shard
		final int[] blocksPerShard = getBlocksPerShard();
		final long[] shardGridPosition = new long[blockGridPosition.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (int)Math.floor((double)blockGridPosition[i] / blocksPerShard[i]);
		}

		return shardGridPosition;
	}

	/**
	 * Returns of the block at the given position relative to this shard, or null if this shard does not contain the given block.
	 *
	 * @return the shard position
	 */
	public long[] getBlockPositionInShard(final long[] shardPosition, final long[] blockPosition) {

		final long[] shardPos = getShardPositionForBlock(blockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] shardSize = getShardSize();
		final long[] blockShardPos = new long[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			blockShardPos[i] = blockPosition[i] % shardSize[i];
		}

		return blockShardPos;
	}

	/**
	 * @return the number of blocks per shard
	 */
	public long getNumBlocks() {

		return Arrays.stream(getBlocksPerShard()).reduce(1, (x, y) -> x * y);
	}

	public static int[] getBlockSize(Codec[] codecs) {

		for (final Codec codec : codecs)
			if (codec instanceof ShardingCodec)
				return ((ShardingCodec)codec).getBlockSize();

		return null;
	}

	public IndexLocation getIndexLocation() {

		return getShardingCodec().getIndexLocation();
	}

	public ShardIndex createIndex() {
		return new ShardIndex(getBlocksPerShard(), getIndexLocation(), getShardingCodec().getIndexCodecs());
	}
}
