package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.util.GridIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//TODO : consider different names? LazyShard? ShardView? PartialReadShard? OnDemand?
public class VirtualShard<T> extends AbstractShard<T> {

	private final ReadData shardData;

	public VirtualShard(
			final DatasetAttributes datasetAttributes,
			long[] gridPosition,
			final ReadData shardData) {

		super(datasetAttributes, gridPosition, null);
		this.shardData = shardData;
	}

	@SuppressWarnings("unchecked")
	public DataBlock<T> getBlock(ReadData blockData, long... blockGridPosition) throws IOException {

		return datasetAttributes.<T>getShardingCodec().getArrayCodec().decode(blockData, blockGridPosition);
	}

	@Override
	public List<DataBlock<T>> getBlocks() {

		return getBlocks(IntStream.range(0, getNumBlocks()).toArray());
	}

	public List<DataBlock<T>> getBlocks(final int[] blockIndexes) {

		// will not contain nulls
		final ShardIndex index = getIndex();
		final ArrayList<DataBlock<T>> blocks = new ArrayList<>();

		if (index.isEmpty())
			return blocks;

		// sort index offsets
		// and keep track of relevant positions
		final long[] indexData = index.getData();
		List<long[]> sortedOffsets = Arrays.stream(blockIndexes)
				.mapToObj(i -> new long[]{indexData[i * 2], i})
				.filter(x -> x[0] != ShardIndex.EMPTY_INDEX_NBYTES)
				.sorted(Comparator.comparingLong(a -> ((long[])a)[0]))
				.collect(Collectors.toList());

		final int nd = getDatasetAttributes().getNumDimensions();
		long[] position = new long[nd];

		final int[] blocksPerShard = getDatasetAttributes().getBlocksPerShard();
		final long[] blockGridMin = IntStream.range(0, nd)
				.mapToLong(i -> blocksPerShard[i] * getGridPosition()[i])
				.toArray();

		for (long[] offsetIndex : sortedOffsets) {
			final long offset = offsetIndex[0];
			if (offset < 0)
				continue;

			final long idx = offsetIndex[1];
			GridIterator.indexToPosition(idx, blocksPerShard, blockGridMin, position);

			final long numBytes = index.getNumBytesByBlockIndex((int)idx);
			//TODO Caleb: Do this with a single access (start at first offset, read through the last)
			try {
				final ReadData blockData = shardData.slice(offset, numBytes);
				final DataBlock<T> block = getBlock(blockData, position.clone());
				blocks.add(block);
			} catch (final N5Exception.N5NoSuchKeyException e) {
				return blocks;
			} catch (final IOException | UncheckedIOException e) {
				throw new N5IOException("Failed to read block from " + Arrays.toString(position), e);
			}
		}
		return blocks;
	}

	@Override
	public DataBlock<T> getBlock(int... relativePosition) {

		final ShardIndex idx = getIndex();
		if (!idx.exists(relativePosition))
			return null;

		final long blockOffset = idx.getOffset(relativePosition);
		final long blockSize = idx.getNumBytes(relativePosition);

		final long[] blockPosInDataset = getDatasetAttributes().getBlockPositionFromShardPosition(getGridPosition(), relativePosition);
		try {
			final ReadData blockData = shardData.slice(blockOffset, blockSize);
			return getBlock(blockData, blockPosInDataset);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + Arrays.toString(blockPosInDataset), e);
		}
	}

	@Override
	public ShardIndex getIndex() {

		index = createIndex();
		try {
			ShardIndex.readFromShard(shardData, index);
		} catch (N5Exception.N5NoSuchKeyException e) {
			return null;
		}
		return index;
	}
}
