package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.output.CountingOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.util.GridIterator;

import static org.janelia.saalfeldlab.n5.N5Exception.*;

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
	default long[] getShardPosition(long... blockPosition) {

		final int[] shardBlockDimensions = getBlockGridSize();
		final long[] shardGridPosition = new long[shardBlockDimensions.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (long)Math.floor((double)(blockPosition[i]) / shardBlockDimensions[i]);
		}

		return shardGridPosition;
	}

	public DataBlock<T> getBlock(long... blockGridPosition);

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

	/**
	 * @return the ShardIndex for this shard, or a new ShardIndex if the Shard is non-existent
	 */
	ShardIndex getIndex();

	/**
	 * @return and empty index of the correct size for the dataset
	 */
	default ShardIndex createIndex() {

		final DatasetAttributes datasetAttributes = getDatasetAttributes();
		return datasetAttributes.getShardingCodec().createIndex(datasetAttributes);
	}

	default ReadData createReadData() throws N5IOException {

		final DatasetAttributes datasetAttributes = getDatasetAttributes();
		final ShardIndex index = ShardIndex.createIndex(datasetAttributes);

		ShardingCodec<T> shardingCodec = datasetAttributes.getShardingCodec();
		final Codec.ArrayCodec<T> arrayCodec = shardingCodec.getArrayCodec();
		long blocksStartBytes = index.getLocation() == ShardingCodec.IndexLocation.START ? index.numBytes() : 0;
		final AtomicLong blockOffset = new AtomicLong(blocksStartBytes);

		if (index.getLocation() == ShardingCodec.IndexLocation.END) {
			return ReadData.from(out -> {
				try (final CountingOutputStream countOut = new CountingOutputStream(out)) {
					long prevCount = 0;
					for (DataBlock<T> block : getBlocks()) {
						arrayCodec.encode(block).writeTo(countOut);
						final int[] blockPosition = getBlockPosition(block.getGridPosition());
						final long curCount = countOut.getByteCount();
						final long blockWrittenSize = curCount - prevCount;
						prevCount = curCount;
						synchronized (index) {
							index.set(blockOffset.getAndAdd(blockWrittenSize), blockWrittenSize, blockPosition);
						}
					}
					synchronized (index) {
						ShardIndex.write(out, index);
					}
				}
			});
		} else {
			final ArrayList<ReadData> blocksData = new ArrayList<>();
			for (DataBlock<T> dataBlock : getBlocks()) {
				ReadData readDataBlock = ReadData.from(out -> arrayCodec.encode(dataBlock).writeTo(out));
				blocksData.add(readDataBlock);
				final long length = readDataBlock.length();
				synchronized (index) {
					index.set(blockOffset.getAndAdd(length), length, getBlockPosition(dataBlock.getGridPosition()));
				}
			}
			return ReadData.from(out -> {
				ShardIndex.write(out, index);
				for (ReadData blockData : blocksData) {
					blockData.writeTo(out);
				}
			});
		}
	}

	class DataBlockIterator<T> implements Iterator<DataBlock<T>> {

		private final GridIterator it;
		private final Shard<T> shard;
		private final ShardIndex index;
		// TODO ShardParameters is deprecated?
		private final DatasetAttributes attributes;
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

			for (int i = blockIndex; i < attributes.getNumBlocksPerShard(); i++) {
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
