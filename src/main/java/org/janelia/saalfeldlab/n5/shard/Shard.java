package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.output.CountingOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
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
	long[] getGridPosition();

	/**
	 * Given a {@code blockPosition} relative to the dataset, return its position relative to this 
	 * shard.
	 *
	 * @param blockPositionInDataset dataset-relative block position
	 * @return the shard-relative block positiorelativeBlockPositionn
	 * @see {@link DatasetAttributes#getBlockPositionInShard(long[], long[])}
	 * @see {@link #getBlockPositionFromShardPosition(long[], long[])}
	 */
	default int[] getRelativeBlockPosition(long... datasetBlockPosition) {

		return getDatasetAttributes().getShardRelativeBlockPosition(
				getGridPosition(),
				datasetBlockPosition);
	}

	/**
	 * Tests whether the block at the {@code relativeBlockPosition} (relative to
	 * this shard) exists.
	 * <p>
	 * Avoids reading the block data, if possible.
	 *
	 * @return true of the block exists in this shard
	 */
	default boolean blockExists(int... relativeBlockPosition) {

		return getIndex().exists(relativeBlockPosition);
	}

	/**
	 * Retrieve the DataBlock at {@code blockGridPosition} relative to this shard if it exists and is
	 * a member of this shard.
	 * <p>
	 * If needed, use {@code getRelativeBlockPosition} to convert block positions relative to the dataset into
	 * positions relative to this shard.
	 *
	 * @param blockGridPosition position of the desired block relative to this shard
	 * @return the block if it exists and is part of this shard, otherwise null
	 */
	DataBlock<T> getBlock(int... blockGridPosition);

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
		ShardingCodec shardingCodec = datasetAttributes.getShardingCodec();
		final BlockCodec<T> arrayCodec = shardingCodec.getDataBlockSerializer();

		final ShardIndex index = createIndex();
		final long indexSize = index.numBytes();
		long blocksStartBytes = index.getLocation() == ShardingCodec.IndexLocation.START ? indexSize : 0;
		final AtomicLong blockOffset = new AtomicLong(blocksStartBytes);

		/* isIndexEmpty is true when writing to a non-sharded dataset through the Shard API. */
		final boolean isIndexEmpty = indexSize == 0;
		if (index.getLocation() == ShardingCodec.IndexLocation.END || isIndexEmpty) {
			return ReadData.from(out -> {
				try (final CountingOutputStream countOut = new CountingOutputStream(out)) {
					long prevCount = 0;
					for (DataBlock<T> block : getBlocks()) {
						arrayCodec.encode(block).writeTo(countOut);
						final int[] blockPosition = getRelativeBlockPosition(block.getGridPosition());
						final long curCount = countOut.getByteCount();
						final long blockWrittenSize = curCount - prevCount;
						prevCount = curCount;
						if (!isIndexEmpty)
							synchronized (index) {
								index.set(blockOffset.getAndAdd(blockWrittenSize), blockWrittenSize, blockPosition);
							}
					}
					if (!isIndexEmpty)
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
					index.set(blockOffset.getAndAdd(length), length, getRelativeBlockPosition(dataBlock.getGridPosition()));
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

			return shard.getBlock(it.nextInt());
		}
	}

}
