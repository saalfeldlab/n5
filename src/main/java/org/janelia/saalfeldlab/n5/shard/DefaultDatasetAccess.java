/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedPosition;

import static org.janelia.saalfeldlab.n5.shard.DatasetAccess.groupInnerPositions;

public class DefaultDatasetAccess<T> implements DatasetAccess<T> {

	private final NestedGrid grid;
	private final BlockCodec<?>[] codecs;
	private final DatasetCodec<?>[] datasetCodecs;

	public DefaultDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] codecs, DatasetCodec<?>[] datasetCodecs ) {
		this.grid = grid;
		this.codecs = codecs;
		this.datasetCodecs = datasetCodecs;
	}

	public DefaultDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] codecs) {
		this( grid, codecs, new DatasetCodec[0]);
	}

	public NestedGrid getGrid() {
		return grid;
	}

	@Override
	public DataBlock<T> readBlock(final PositionValueAccess pva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = grid.nestedPosition(gridPosition);
		return (DataBlock<T>)decodeWithDatasetCodecs(readBlockRecursive(pva.get(position.key()), position, grid.numLevels() - 1));
	}

	private DataBlock<T> readBlockRecursive(
			final ReadData readData,
			final NestedPosition position,
			final int level) {
		if (readData == null) {
			return null;
		} else if (level == 0) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			return codec.decode(readData, position.absolute(0));
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final RawShard shard = codec.decode(readData, position.absolute(level)).getData();
			return readBlockRecursive(shard.getElementData(position.relative(level - 1)), position, level - 1);
		}
	}

	@Override
	public List<DataBlock<T>> readBlocks(PositionValueAccess pva, List<long[]> positions) throws N5IOException {

		if (grid.numLevels() == 1) {
			return positions.stream().map(it -> readBlock(pva, it)).collect(Collectors.toList());
		}

		final List<NestedPosition> blockPositions = positions.stream().map(grid::nestedPosition).collect(Collectors.toList());

		final int outermostLevel = grid.numLevels() - 1;
		final Collection<List<NestedPosition>> blocksPerOutermostShard = groupInnerPositions(grid, blockPositions, outermostLevel);

		final ArrayList<DataBlock<T>> blocks = new ArrayList<>(blockPositions.size());
		for (List<NestedPosition> blocksInSingleShard : blocksPerOutermostShard) {
			if (blocksInSingleShard.isEmpty())
				continue;

			final NestedPosition firstBlock = blocksInSingleShard.get(0);
			final ReadData readData = pva.get(firstBlock.key());
			final List<DataBlock<T>> shardBlocks;
			try {
				shardBlocks = readShardRecursive(readData, blocksInSingleShard, outermostLevel);
			} catch (N5NoSuchKeyException e) {
				continue;
			}

			blocks.addAll(shardBlocks);
		}

		//TODO Caleb: No guarantee of order; If we want that, we need to sort the result.
		return blocks;
	}

	/**
	 * Bulk Read operation on a shard. `positions` MUST all be in the same shard.
	 * That is, for each `position` in `positions`, `position.absolute(level)` must be the same.
	 *
	 * @param readData for the corresponding shard
	 * @param positions of blocks within the shard to be read
	 * @param level of the shard
	 * @return list of blocks read from a single shard
	 */
	private List<DataBlock<T>> readShardRecursive(
			final ReadData readData,
			final List<NestedPosition> positions,
			final int level
	) {
		// Cannot have a shard at level 0
		if (readData == null || level == 0) {
			return null;
		}

		if (positions.isEmpty()) {
			return Collections.emptyList();
		}

		final NestedPosition firstBlock = positions.get(0);
		final long[] shardPosition = firstBlock.absolute(level);

		@SuppressWarnings("unchecked")
		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
		final RawShard shard = codec.decode(readData, shardPosition).getData();

		final ArrayList<DataBlock<T>> blocks = new ArrayList<>(positions.size());
		if (level == 1) {
			final int innerMostLevel = 0;
			//Base case; read the blocks
			for (NestedPosition blockPosition : positions) {
				final long[] elementPos = blockPosition.relative(innerMostLevel);
				final ReadData elementData = shard.getElementData(elementPos);
				final DataBlock<T> block = readBlockRecursive(elementData, blockPosition, innerMostLevel);
				blocks.add(block);
			}
		} else {
			// group the blocks by shard for next level, and call again for each nested shard
			final Collection<List<NestedPosition>> nextLevelShards = groupInnerPositions(grid, positions, level - 1);
			for (List<NestedPosition> innerPositions : nextLevelShards) {
				final List<DataBlock<T>> innerBlocks = readShardRecursive(readData, innerPositions, level - 1);
				blocks.addAll(innerBlocks);
			}
		}

		return blocks;
	}

	@Override
	public void writeBlock(final PositionValueAccess pva, final DataBlock<T> dataBlockArg) throws N5IOException {

		@SuppressWarnings("unchecked")
		final DataBlock<T> dataBlock = (DataBlock<T>)encodeWithDatasetCodecs(dataBlockArg);
		final NestedPosition position = grid.nestedPosition(dataBlock.getGridPosition());
		final long[] key = position.key();

		final ReadData existingData = getExistingReadData(pva, key);
		final ReadData modifiedData = writeBlockRecursive(existingData, dataBlock, position, grid.numLevels() - 1);
		pva.put(key, modifiedData);
	}

	private ReadData writeBlockRecursive(
			final ReadData existingReadData,
			final DataBlock<T> dataBlock,
			final NestedPosition position,
			final int level) {
		if (level == 0) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			return codec.encode(dataBlock);
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = existingReadData == null ?
					new RawShard(grid.relativeBlockSize(level)) :
					codec.decode(existingReadData, gridPos).getData();
			final long[] elementPos = position.relative(level - 1);
			final ReadData existingElementData = (level == 1)
					? null // if level == 1, we don't need to extract the nested (DataBlock<T>) ReadData because it will be overridden anyway
					: shard.getElementData(elementPos);
			final ReadData modifiedElementData = writeBlockRecursive(existingElementData, dataBlock, position, level - 1);
			shard.setElementData(modifiedElementData, elementPos);
			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	@Override
	public void writeBlocks(final PositionValueAccess pva, final List<DataBlock<T>> dataBlocks) throws N5IOException {

		if (grid.numLevels() == 1) {
			dataBlocks.forEach(it -> writeBlock(pva, it));
			return;
		}

		final List<NestedPositionDataBlock<T>> nestedPositionDataBlocks = dataBlocks.stream()
				.map(it -> new NestedPositionDataBlock<>(grid, it))
				.collect(Collectors.toList());

		final int outermostLevel = grid.numLevels() - 1;
		final Collection<List<NestedPositionDataBlock<T>>> dataBlocksPerShard = groupInnerPositions(grid, nestedPositionDataBlocks, outermostLevel);

		for (List<NestedPositionDataBlock<T>> dataBlocksForShard : dataBlocksPerShard) {
			if (dataBlocksForShard.isEmpty())
				continue;

			final NestedPositionDataBlock<T> firstDataBlock = dataBlocksForShard.get(0);
			final long[] shardKey = firstDataBlock.key();

			//TODO Caleb: When writing all blocks in a shard, we don't need to read existing data.
			//	Also, could only materialize the index and skip reading if we are overwriting all existing blocks.
			final ReadData existingReadData = getExistingReadData(pva, shardKey);
			final ReadData writeShardReadData = writeShardRecursive(existingReadData, dataBlocksForShard, outermostLevel);

			pva.put(shardKey, writeShardReadData);
		}
	}

	private ReadData writeShardRecursive(
			final ReadData existingShard,
			final List<NestedPositionDataBlock<T>> dataBlocks,
			final int level) {

		// cannot have shard level 0, or nothing to write
		if (level == 0 || dataBlocks.isEmpty()) {
			return null;
		}

		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>)codecs[level];
		final NestedPositionDataBlock<T> firstBlock = dataBlocks.get(0);
		final long[] shardPosition = firstBlock.absolute(level);

		final RawShard shard;
		if (existingShard == null)
			shard = new RawShard(grid.relativeBlockSize(level));
		else
			shard = codec.decode(existingShard, shardPosition).getData();

		if (level == 1) {
			final int innerMostLevel = 0;
			// Base case, write the blocks
			for (NestedPositionDataBlock<T> nestedPosDataBlock : dataBlocks) {
				final DataBlock<T> dataBlock = nestedPosDataBlock.getDataBlock();

				final long[] blockRelativePos = nestedPosDataBlock.relative(innerMostLevel);
				final ReadData blockReadData = shard.getElementData(blockRelativePos);
				final ReadData modifiedShardBlock = writeBlockRecursive(blockReadData, dataBlock, nestedPosDataBlock, innerMostLevel);
				shard.setElementData(modifiedShardBlock, blockRelativePos);
			}
		} else {
			final Collection<List<NestedPositionDataBlock<T>>> dataBlocksForInnerShards = groupInnerPositions(grid, dataBlocks, level - 1);
			for (List<NestedPositionDataBlock<T>> innerShardDataBlocks : dataBlocksForInnerShards) {
				if (innerShardDataBlocks.isEmpty())
					continue;

				final ReadData innerShardReadData = writeShardRecursive(existingShard, innerShardDataBlocks, level - 1);

				final NestedPositionDataBlock<T> firstInnerNestedPosBlock = innerShardDataBlocks.get(0);
				final long[] relPosInShard = firstInnerNestedPosBlock.relative(level - 1);
				shard.setElementData(innerShardReadData, relPosInShard);
			}
		}
		return codec.encode(new RawShardDataBlock(shardPosition, shard));
	}

	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> blocks,
			final long[] datasetDimensions,
			final boolean writeFully
	) throws N5IOException {

		final Region region = new Region(min, size, grid, datasetDimensions);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
			final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
			final ReadData existingData = nestedWriteFully ? null : getExistingReadData(pva, key);
			final ReadData modifiedData = writeRegionRecursive(existingData, region, blocks, pos);
			pva.put(key, modifiedData);
		}
	}

	@Override
	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> blocks,
			final long[] datasetDimensions,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		final Region region = new Region(min, size, grid, datasetDimensions);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			exec.submit(() -> {
				final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
				final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
				final ReadData existingData = nestedWriteFully ? null : getExistingReadData(pva, key);
				final ReadData modifiedData = writeRegionRecursive(existingData, region, blocks, pos);
				pva.put(key, modifiedData);
			});
		}
	}

	private ReadData writeRegionRecursive(
			final ReadData existingReadData, // may be null
			final Region region,
			final DataBlockSupplier<T> blocks,
			final NestedPosition position
	) {
		final boolean writeFully = existingReadData == null;
		final int level = position.level();
		if ( level == 0 ) {

			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			final long[] gridPosition = position.absolute(0);

			// If the DataBlock is not fully contained in the region, we will
			// get existingReadData != null. In that case, we decode the
			// existing DataBlock and pass it to the BlockSupplier for modification.
			final DataBlock<T> existingDataBlock = (existingReadData == null)
					? null
					: codec.decode(existingReadData, gridPosition);
			final DataBlock<T> dataBlock = blocks.get(gridPosition, existingDataBlock);

			return codec.encode(dataBlock);
		} else {

			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = existingReadData == null ?
					new RawShard(grid.relativeBlockSize(level)) :
					codec.decode(existingReadData, gridPos).getData();
			for (NestedPosition pos : region.containedNestedPositions(position)) {
				final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
				final long[] elementPos = pos.relative();
				final ReadData existingElementData = nestedWriteFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = writeRegionRecursive(existingElementData, region, blocks, pos);
				shard.setElementData(modifiedElementData, elementPos);
			}

			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	@Override
	public boolean deleteBlock(final PositionValueAccess pva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = grid.nestedPosition(gridPosition);
		final long[] key = position.key();
		if (grid.numLevels() == 1) {
			// for non-sharded dataset, don't bother getting the value, just remove the key.
			try {
				return pva.remove(key);
			} catch (final Exception e) {
				throw new N5Exception("The shard at " + Arrays.toString(key) + " could not be deleted.", e);
			}
		} else {
			final ReadData existingData = pva.get(key);
			final ReadData modifiedData = deleteBlockRecursive(existingData, position, grid.numLevels() - 1);
			if (existingData != null && modifiedData == null) {
				return pva.remove(key);
			} else if (modifiedData != existingData) {
				pva.put(key, modifiedData);
				return true;
			} else {
				return false;
			}
		}
	}

	private ReadData deleteBlockRecursive(
			final ReadData existingReadData,
			final NestedPosition position,
			final int level) {
		if (level == 0 || existingReadData == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = codec.decode(existingReadData, gridPos).getData();
			final long[] elementPos = position.relative(level - 1);
			final ReadData existingElementData = shard.getElementData(elementPos);
			if (existingElementData == null) {
				// The DataBlock (or the whole nested shard containing it) does not exist.
				// This shard remains unchanged.
				return existingReadData;
			} else {
				final ReadData modifiedElementData = deleteBlockRecursive(existingElementData, position, level - 1);
				if (modifiedElementData == existingElementData) {
					// The nested shard was not modified.
					// This shard remains unchanged.
					return existingReadData;
				}
				shard.setElementData(modifiedElementData, elementPos);
				if (modifiedElementData == null) {
					// The DataBlock or nested shard was removed.
					// Check whether this shard becomes empty.
					if (shard.index().allElementsNull()) {
						// This shard is empty and should be removed.
						return null;
					}
				}
				return codec.encode(new RawShardDataBlock(gridPos, shard));
			}
		}
	}

	private static ReadData getExistingReadData(final PositionValueAccess pva, final long[] key) {
		// need to read the shard anyway, and currently (Sept 24 2025)
		// have no way to tell if the key exists from what is in this method except to attempt
		// to materialize and catch the N5NoSuchKeyException
		try {
			ReadData existingData = pva.get(key);
			if (existingData != null)
				existingData.materialize();
			return existingData;
		} catch (N5NoSuchKeyException e) {
			return null;
		}
	}

	/**
	 * NestedPosition wrapper for a DataBlock<T>. Useful for grouping DataBlock<T> by nested shard position.
	 *
	 * @param <T> type of the datablock
	 */
	private static class NestedPositionDataBlock<T> extends NestedPosition {

		private final DataBlock<T> dataBlock;

		private NestedPositionDataBlock(NestedGrid grid, DataBlock<T> dataBlock) {

			super(grid, dataBlock.getGridPosition(), 0);
			this.dataBlock = dataBlock;
		}

		private DataBlock<T> getDataBlock() {

			return dataBlock;
		}
	}

	private DataBlock<?> decodeWithDatasetCodecs(DataBlock<?> block) {

		DataBlock<?> result = block;
		for (DatasetCodec<?> codec : datasetCodecs) {
			result = codec.decode(result);
		}
		return result;
	}

	private DataBlock<?> encodeWithDatasetCodecs(DataBlock<?> block) {

		DataBlock<?> result = block;
		for (DatasetCodec codec : datasetCodecs) {
			result = codec.encode(result);
		}
		return result;
	}


}
