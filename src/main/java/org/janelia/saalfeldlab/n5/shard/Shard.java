package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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

	public <A extends DatasetAttributes & ShardParameters> A getDatasetAttributes();

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

	default int getNumBlocks() {

		return Arrays.stream(getBlockGridSize()).reduce(1, (x, y) -> x * y);
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
		long[] min = getDatasetAttributes().getBlockPositionFromShardPosition( getGridPosition(), new long[nd]);
		return new GridIterator(GridIterator.int2long(getBlockGridSize()), min);
	}

	public ShardIndex getIndex();

	public static <T,A extends DatasetAttributes & ShardParameters> Shard<T> createEmpty(final A attributes, long... shardPosition) {

		final long[] emptyIndex = new long[(int)(2 * attributes.getNumBlocks())];
		Arrays.fill(emptyIndex, ShardIndex.EMPTY_INDEX_NBYTES);
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
		private final ShardIndex index;
		private final ShardParameters attributes;
		private int blockIndex = 0;

		public DataBlockIterator(final Shard<T> shard) {

			this.shard = shard;
			this.index = shard.getIndex();
			this.attributes = shard.getDatasetAttributes();
			this.blockIndex = 0;
			it = new GridIterator(shard.getBlockGridSize());
		}

		@Override
		public boolean hasNext() {

			for (int i = blockIndex; i < attributes.getNumBlocks(); i++) {
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
