package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Objects;

import org.janelia.saalfeldlab.n5.BlockParameters;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;

import javax.annotation.CheckForNull;

public interface ShardParameters extends BlockParameters {

	/**
	 * The size of the blocks in pixel units.
	 *
	 * @return the number of pixels per dimension for this shard.
	 */
	@CheckForNull
	public int[] getShardSize();

	/**
	 * Returns the number of blocks per dimension for a shard.
	 *
	 * @return the size of the block grid of a shard
	 */
	default int[] getBlocksPerShard() {

		final int[] shardSize = getShardSize();
		Objects.requireNonNull(shardSize, "getShardSize() must not be null");
		final int nd = getNumDimensions();
		final int[] blocksPerShard = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			blocksPerShard[i] = shardSize[i] / blockSize[i];

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
		Objects.requireNonNull(shardSize, "getShardSize() must not be null");
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

	default Map<Position, List<long[]>> groupBlockPositions(final List<long[]> blockPositions) {

		final TreeMap<Position, List<long[]>> map = new TreeMap<>();
		for( final long[] blockPos : blockPositions ) {
			Position shardPos = Position.wrap(getShardPositionForBlock(blockPos));
			if( !map.containsKey(shardPos)) {
				map.put(shardPos, new ArrayList<>());
			}
			map.get(shardPos).add(blockPos);
		}

		return map;
	}

	default <T> Map<Position, List<DataBlock<T>>> groupBlocks(final List<DataBlock<T>> blocks) {

		// figure out how to re-use groupBlockPositions here?
		final TreeMap<Position, List<DataBlock<T>>> map = new TreeMap<>();
		for (final DataBlock<T> block : blocks) {
			Position shardPos = Position.wrap(getShardPositionForBlock(block.getGridPosition()));
			if (!map.containsKey(shardPos)) {
				map.put(shardPos, new ArrayList<>());
			}
			map.get(shardPos).add(block);
		}

		return map;
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
