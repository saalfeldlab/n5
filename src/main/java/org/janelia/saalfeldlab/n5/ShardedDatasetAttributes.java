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

		if (!validateShardBlockSize(shardSize, blockSize)) {
			throw new N5Exception(String.format("Invalid shard %s / block size %s",
					Arrays.toString(shardSize),
					Arrays.toString(blockSize)));
		}

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

	/**
	 * Returns whether the given shard and block sizes are valid. Specifically, is
	 * the shard size a multiple of the block size in every dimension.
	 *
	 * @param shardSize size of the shard in pixels
	 * @param blockSize size of a block in pixels
	 * @return
	 */
	public static boolean validateShardBlockSize(final int[] shardSize, final int[] blockSize) {

		if (shardSize.length != blockSize.length)
			return false;

		for (int i = 0; i < shardSize.length; i++) {
			if (shardSize[i] % blockSize[i] != 0)
				return false;
		}
		return true;
	}

	public ShardedDatasetAttributes getShardAttributes() {
		return this;
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

	@Override
	protected Codec[] concatenateCodecs() {

		return new Codec[] { shardingCodec };
	}

	/**
	 * The size of the blocks in pixel units.
	 *
	 * @return the number of pixels per dimension for this shard.
	 */
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
	 * Returns the block at the given position relative to this shard, or null if this shard does not contain the given block.
	 *
	 * @return the block position
	 */
	public long[] getBlockPositionInShard(final long[] shardPosition, final long[] blockPosition) {

		// TODO check correctness 
		final long[] shardPos = getShardPositionForBlock(blockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] shardSize = getBlocksPerShard();
		final long[] blockShardPos = new long[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			blockShardPos[i] = blockPosition[i] % shardSize[i];
		}

		return blockShardPos;
	}

	/**
	 * Given a block's position relative to a shard, returns its position in pixels
	 * relative to the image.
	 *
	 * @return the block position
	 */
	public long[] getBlockMinFromShardPosition(final long[] shardPosition, final long[] blockPosition) {

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
	 * @return the block position
	 */
	public long[] getBlockPositionFromShardPosition(final long[] shardPosition, final long[] blockPosition) {

		// is this useful?
		final int[] shardBlockSize = getBlocksPerShard();
		final long[] blockImagePos = new long[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			blockImagePos[i] = (shardPosition[i] * shardBlockSize[i]) + (blockPosition[i]);
		}

		return blockImagePos;
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
