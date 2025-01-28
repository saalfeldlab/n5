package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.janelia.saalfeldlab.n5.BlockParameters;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;

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
		return new ShardIndex(getBlocksPerShard(), getShardingCodec().getIndexCodecs());
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
	 * Returns the number of blocks per dimension that tile the image.
	 *
	 * @return blocks per image
	 */
	default long[] blocksPerImage() {
		return IntStream.range(0, getNumDimensions()).mapToLong(i -> {
			return (long) Math.ceil(getDimensions()[i] / getBlockSize()[i]);
		}).toArray();
	}

	/**
	 * Returns the number of shards per dimension that tile the image.
	 *
	 * @return shards per image
	 */
	default long[] shardsPerImage() {
		return IntStream.range(0, getNumDimensions()).mapToLong(i -> {
			return (long)Math.ceil(getDimensions()[i] / getShardSize()[i]);
		}).toArray();
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

		final int[] blocksPerShard = getBlocksPerShard();
		final long[] shardPos = Shard.getShardPositionForBlock(blocksPerShard, blockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] blockShardPos = new int[blocksPerShard.length];
		for (int i = 0; i < blocksPerShard.length; i++) {
			blockShardPos[i] = (int)(blockPosition[i] % blocksPerShard[i]);
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

	default <T> Map<Position, List<T>> groupByBlockPositions(final List<T> positionables,
			final Function<T, long[]> getPosition) {

		final int[] blocksPerShard = getBlocksPerShard();	
		final TreeMap<Position, List<T>> map = new TreeMap<>();
		for (final T t : positionables) {
			final Position shardPos = Position.wrap(
					Shard.getShardPositionForBlock(blocksPerShard, getPosition.apply(t)));
			if (!map.containsKey(shardPos)) {
				map.put(shardPos, new ArrayList<>());
			}
			map.get(shardPos).add(t);
		}

		return map;
	}

	default Map<Position, List<long[]>> groupBlockPositions(final List<long[]> blockPositions) {

		return groupByBlockPositions(blockPositions, x -> x);
	}
	
	default <T> Map<Position, List<DataBlock<T>>> groupBlocks(final List<DataBlock<T>> blocks) {

		return groupByBlockPositions(blocks, x -> x.getGridPosition());
	}

	/**
	 * @return the number of blocks per shard
	 */
	default long getNumBlocks() {

		return Arrays.stream(getBlocksPerShard()).reduce(1, (x, y) -> x * y);
	}

	default Stream<long[]> blockPositions() {

		final int[] blocksPerShard = getBlocksPerShard();
		return toStream( new GridIterator(shardsPerImage()))
				.flatMap( shardPosition -> {
					final int nd = getNumDimensions();
					final long[] min = getBlockPositionFromShardPosition(shardPosition, new long[nd]);
					return toStream(new GridIterator(GridIterator.int2long(blocksPerShard), min));
				});
	}

	static <T> Stream<T> toStream( final Iterator<T> it ) {
		return StreamSupport.stream( Spliterators.spliteratorUnknownSize(
				  it, Spliterator.ORDERED),
		          false);
	}

}
