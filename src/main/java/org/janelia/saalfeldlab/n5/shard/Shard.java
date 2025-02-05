package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public interface Shard<T> extends Iterable<DataBlock<T>> {

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

	DatasetAttributes getDatasetAttributes();

	/**
	 * Returns the size of this shard in pixels.
	 *
	 * The size of a shard is expected to be smaller than or equal to the
	 * spacing of the shard grid. The dimensionality of size is expected to be
	 * equal to the dimensionality of the dataset. Consistency is not enforced.
	 *
	 * @return size of the 
	 */
	public int[] getSize();

	/**
	 * Returns the size of sub-blocks in pixel units.
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
	 * Returns the number of blocks this shard contains along all dimensions.
	 *
	 * The size of a shard expected to be smaller than or equal to the spacing of the shard grid. The dimensionality of size is expected to be equal to the dimensionality of the
	 * dataset. Consistency is not enforced.
	 *
	 * @return size of the shard in units of blocks
	 */
	default int[] getBlocksPerShard() {

		final int nd = getSize().length;
		final int[] blocksPerShard = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			blocksPerShard[i] = getSize()[i] / blockSize[i];

		return blocksPerShard;
	}

	/**
	 * Given an absolute block position, returns that block's position 
	 * relative to this shard or null if this shard does not contain the given block.
	 *
	 * @return block positionr relative to this shard
	 */
	default int[] relativeBlockPosition(long... blockPosition) {

		final long[] shardPos = getShardPositionForBlock(blockPosition);
		return getRelativeBlockPosition(getBlocksPerShard(), shardPos, blockPosition);
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
	default long[] getShardPosition(long... blockPosition) {

		final int[] blocksPerShard = getBlocksPerShard();
		final long[] shardGridPosition = new long[blocksPerShard.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (long)Math.floor((double)(blockPosition[i]) / blocksPerShard[i]);
		}

		return shardGridPosition;
	}

	public DataBlock<T> getBlock(long... blockGridPosition);

//	public void writeBlock(DataBlock<T> block);

	//TODO Caleb: add writeBlocks that does NOT always expect to overwrite the entire existing Shard

	default Iterator<DataBlock<T>> iterator() {

		return new DataBlockIterator<>(this);
	}
	
	/**
	 * Returns the number of elements in this {@link Shard}. This number is
	 * not necessarily equal {@link #getNumElements(int[])
	 * getNumElements(getSize())}.
	 *
	 * @return the number of elements
	 */
	default int getNumElements() {
		return Arrays.stream(getBlocksPerShard()).reduce(1, (x, y) -> x * y);
	}

	default List<DataBlock<T>> getBlocks() {

		final List<DataBlock<T>> blocks = new ArrayList<>();
		for (DataBlock<T> block : this) {
			blocks.add(block);
		}
		return blocks;
	}

	/**
	 * Returns an {@link Iterator} over block positions contained in this shard.
	 * 
	 * @return
	 */
	default Iterator<long[]> blockPositionIterator() {

		final int nd = getSize().length;
		final long[] min = getAbsoluteBlockPosition( 
				getBlocksPerShard(),
				getGridPosition(),
				new long[nd]);
		return new GridIterator(GridIterator.int2long(getBlocksPerShard()), min);
	}

	ShardIndex getIndex();

	static <T> Shard<T> createEmpty(final DatasetAttributes attributes, long... shardPosition) {

		final long[] emptyIndex = new long[(int)(2 * attributes.getNumBlocks())];
		Arrays.fill(emptyIndex, ShardIndex.EMPTY_INDEX_NBYTES);
		final ShardIndex shardIndex = new ShardIndex(attributes.getBlocksPerShard(), emptyIndex);
		return new InMemoryShard<T>(attributes, shardPosition, shardIndex);
	}

	/**
	 * Given a block's position relative to the array, returns the position of the
	 * shard containing that block relative to the shard grid.
	 *
	 * @param blocksPerShard     size of the shards block grid
	 * @param blockGridPosition position of a block relative to the array
	 * @return the position of the containing shard in the shard grid
	 */
	static long[] getShardPositionForBlock(
			final int[] blocksPerShard,
			final long[] blockGridPosition) {

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
	static int[] getRelativeBlockPosition(
			final int[] blocksPerShard,
			final long[] shardPosition,
			final long[] absoluteBlockPosition) {

		final long[] shardPos = Shard.getShardPositionForBlock(blocksPerShard, absoluteBlockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] blockShardPos = new int[blocksPerShard.length];
		for (int i = 0; i < blocksPerShard.length; i++) {
			blockShardPos[i] = (int)(absoluteBlockPosition[i] % blocksPerShard[i]);
		}

		return blockShardPos;
	}

	/**
	 * Given a block's position relative to a shard, returns its position relative
	 * to the image.
	 *
	 */
	static long[] getAbsoluteBlockPosition(
			final int[] blocksPerShard, final long[] shardPosition, final long[] relativeBlockPosition) {

		// is this useful?
		final int nd = relativeBlockPosition.length;
		final long[] blockImagePos = new long[nd];
		for (int i = 0; i < nd; i++) {
			blockImagePos[i] = (shardPosition[i] * blocksPerShard[i]) + (relativeBlockPosition[i]);
		}

		return blockImagePos;
	}

	class DataBlockIterator<T> implements Iterator<DataBlock<T>> {

		private final GridIterator it;
		private final Shard<T> shard;
		private final ShardIndex index;
		// TODO ShardParameters is deprecated?
		private int blockIndex = 0;

		public DataBlockIterator(final Shard<T> shard) {

			this.shard = shard;
			this.index = shard.getIndex();
			this.blockIndex = 0;
			it = new GridIterator(shard.getBlocksPerShard());
		}

		@Override
		public boolean hasNext() {

			for (int i = blockIndex; i < shard.getNumElements(); i++) {
				if (index.exists(i))
					return true;
			}
			return false;
		}

		@Override
		public DataBlock<T> next() {
			while (!index.exists(blockIndex++))
				it.fwd();

			return shard.getBlock(it.next());
		}
	}

}
