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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.util.SubArrayCopy;

public class DefaultDatasetAccess<T> implements DatasetAccess<T> {

	private final NestedGrid grid;
	private final BlockCodec<?>[] codecs;

	public DefaultDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] codecs) {
		this.grid = grid;
		this.codecs = codecs;
	}

	public NestedGrid getGrid() {
		return grid;
	}

	//
	// -- readBlock -----------------------------------------------------------

	@Override
	public DataBlock<T> readBlock(final PositionValueAccess pva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = grid.nestedPosition(gridPosition);
		try {
			return readBlockRecursive(pva.get(position.key()), position, grid.numLevels() - 1);
		} catch (N5NoSuchKeyException ignored) {
			return null;
		}
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

	//
	// -- readBlocks ----------------------------------------------------------

	@Override
	public List<DataBlock<T>> readBlocks(final PositionValueAccess pva, final List<long[]> gridPositions) throws N5IOException {

		// for non-sharded datasets, just read the blocks individually
		if (grid.numLevels() == 1) {
			return gridPositions.stream().map(pos -> readBlock(pva, pos)).collect(Collectors.toList());
		}

		// Create a list of DataBlockRequests and sort it such that requests
		// from the same (nested) shard are grouped contiguously.
		final DataBlockRequests<T> requests = createReadRequests(gridPositions);
		final List<DataBlockRequest<T>> duplicates = requests.removeDuplicates();

		final List<DataBlockRequests<T>> split = requests.split();
		for (final DataBlockRequests<T> subRequests : split) {
			final long[] key = subRequests.relativeGridPosition();
			final ReadData readData = pva.get(key);
			readBlocksRecursive(readData, subRequests);
		}

		return requests.blocks(duplicates);
	}

	/**
	 * Bulk Read operation on a shard.
	 *
	 * @param readData for the corresponding shard
	 * @param requests for blocks within the shard to be read
	 */
	private void readBlocksRecursive(
			final ReadData readData,
			final DataBlockRequests<T> requests
	) {
		assert !requests.requests.isEmpty();
		assert requests.level > 0;

		if (readData == null) {
			return;
		}

		final int level = requests.level();
		@SuppressWarnings("unchecked")
		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
		final RawShard shard = codec.decode(readData, requests.gridPosition()).getData();

		if (level == 1 ) {
			//Base case; read the blocks
			for (final DataBlockRequest<T> request : requests) {
				final long[] elementPos = request.position.relative(0);
				final ReadData elementData = shard.getElementData(elementPos);
				request.block = readBlockRecursive(elementData, request.position, 0);
			}
		} else { // level > 1
			final List<DataBlockRequests<T>> split = requests.split();
			for (final DataBlockRequests<T> subRequests : split) {
				final long[] subShardPosition = subRequests.relativeGridPosition();
				final ReadData elementData = shard.getElementData(subShardPosition);
				readBlocksRecursive(elementData, subRequests);
			}
		}
	}

	//
	// -- writeBlock ----------------------------------------------------------

	@Override
	public void writeBlock(final PositionValueAccess pva, final DataBlock<T> dataBlock) throws N5IOException {

		final NestedPosition position = grid.nestedPosition(dataBlock.getGridPosition());
		final long[] key = position.key();

		final ReadData existingData = getExistingReadData(pva, key);
		final ReadData modifiedData = writeBlockRecursive(existingData, dataBlock, position, grid.numLevels() - 1);
		pva.set(key, modifiedData);
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

	//
	// -- writeBlocks ---------------------------------------------------------

	@Override
	public void writeBlocks(final PositionValueAccess pva, final List<DataBlock<T>> dataBlocks) throws N5IOException {

		if (grid.numLevels() == 1) {
			dataBlocks.forEach(it -> writeBlock(pva, it));
			return;
		}

		// Create a list of DataBlockRequests, sorted such that requests from
		// the same (nested) shard are grouped contiguously.
		final DataBlockRequests<T> requests = createWriteRequests(dataBlocks);
		requests.removeDuplicates();
		final List<DataBlockRequests<T>> split = requests.split();
		for (final DataBlockRequests<T> subRequests : split) {
			final boolean writeFully = subRequests.coversShard();
			final long[] shardKey = subRequests.relativeGridPosition();
			final ReadData existingReadData = writeFully ? null : getExistingReadData(pva, shardKey);
			final ReadData writeShardReadData = writeBlocksRecursive(existingReadData, subRequests);
			pva.set(shardKey, writeShardReadData);
		}
	}

	/**
	 * Bulk Write operation on a shard.
	 *
	 * @param existingReadData encoded existing shard data (to decode and partially override)
	 * @param requests for blocks within the shard to be written
	 */
	private ReadData writeBlocksRecursive(
			final ReadData existingReadData, // may be null
			final DataBlockRequests<T> requests
	) {
		assert !requests.requests.isEmpty();
		assert requests.level > 0;

		final boolean writeFully = existingReadData == null;

		final int level = requests.level();
		@SuppressWarnings("unchecked")
		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
		final long[] gridPos = requests.gridPosition();
		final RawShard shard = writeFully ?
				new RawShard(grid.relativeBlockSize(level)) :
				codec.decode(existingReadData, gridPos).getData();

		if ( level == 1 ) {
			// Base case, write the blocks
			for (final DataBlockRequest<T> request : requests) {
				final ReadData elementData = writeBlockRecursive(null, request.block, request.position, 0);
				final long[] elementPos = request.position.relative(0);
				shard.setElementData(elementData, elementPos);
			}
		} else { // level > 1
			final List<DataBlockRequests<T>> split = requests.split();
			for (final DataBlockRequests<T> subRequests : split) {
				final boolean nestedWriteFully = writeFully || subRequests.coversShard();
				final long[] elementPos = subRequests.relativeGridPosition();
				final ReadData existingElementData = nestedWriteFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = writeBlocksRecursive(existingElementData, subRequests);
				shard.setElementData(modifiedElementData, elementPos);
			}
		}

		return codec.encode(new RawShardDataBlock(gridPos, shard));
	}

	//
	// -- writeRegion ---------------------------------------------------------

	@Override
	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> blocks,
			final boolean writeFully
	) throws N5IOException {

		final Region region = new Region(min, size, grid);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
			final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
			final ReadData existingData = nestedWriteFully ? null : getExistingReadData(pva, key);
			final ReadData modifiedData = writeRegionRecursive(existingData, region, blocks, pos);
			pva.set(key, modifiedData);
		}
	}

	@Override
	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> blocks,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		final Region region = new Region(min, size, grid);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			exec.submit(() -> {
				final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
				final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
				final ReadData existingData = nestedWriteFully ? null : getExistingReadData(pva, key);
				final ReadData modifiedData = writeRegionRecursive(existingData, region, blocks, pos);
				pva.set(key, modifiedData);
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

			// null blocks may be provided when they contain only the fill value
			// and only non-empty blocks should be written, for example
			if (dataBlock == null)
				return null;

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

			// do not write empty shards
			if (shard.isEmpty())
				return null;

			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	//
	// -- deleteBlock ---------------------------------------------------------

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
			final ReadData existingData = pva.get(key); // TODO: use getExistingReadData() instead !?
			final ReadData modifiedData = deleteBlockRecursive(existingData, position, grid.numLevels() - 1);
			if (existingData != null && modifiedData == null) {
				return pva.remove(key);
			} else if (modifiedData != existingData) {
				// Here, we are about to write the shard data, but without the block to be deleted.
				// Need to make sure that the read operations happen now before pva.set acquires a write lock
				modifiedData.materialize();
				pva.set(key, modifiedData);
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
					if (shard.isEmpty()) {
						// This shard is empty and should be removed.
						return null;
					}
				}
				return codec.encode(new RawShardDataBlock(gridPos, shard));
			}
		}
	}

	//
	// -- deleteBlocks --------------------------------------------------------

	@Override
	public boolean deleteBlocks(final PositionValueAccess pva, final List<long[]> gridPositions) throws N5IOException {

		boolean deleted = false;

		// for non-sharded datasets, just delete the blocks individually
		if (grid.numLevels() == 1) {
			for (long[] pos : gridPositions) {
				deleted |= deleteBlock(pva, pos);
			}
			return deleted;
		}

		// Create a list of DataBlockRequests and sort it such that requests
		// from the same (nested) shard are grouped contiguously.
		// Despite the name, createReadRequests() works for delete requests as well ...
		final DataBlockRequests<T> requests = createReadRequests(gridPositions);
		requests.removeDuplicates();

		final List<DataBlockRequests<T>> split = requests.split();
		for (final DataBlockRequests<T> subRequests : split) {
			final boolean writeFully = subRequests.coversShard();
			final long[] key = subRequests.relativeGridPosition();
			final ReadData existingData = writeFully ? null : getExistingReadData(pva, key);
			final ReadData modifiedData = deleteBlocksRecursive(existingData, subRequests);
			if (existingData != null && modifiedData == null) {
				deleted |= pva.remove(key);
			} else if (modifiedData != existingData) {
				pva.set(key, modifiedData);
				deleted = true;
			}
		}

		return deleted;
	}

	/**
	 * Bulk Delete operation on a shard.
	 *
	 * @param existingReadData encoded existing shard data (to decode and partially override)
	 * @param requests for blocks within the shard to be deleted
	 */
	private ReadData deleteBlocksRecursive(
			final ReadData existingReadData, // may be null
			final DataBlockRequests<T> requests
	) {
		assert !requests.requests.isEmpty();
		assert requests.level > 0;

		if (existingReadData == null) {
			return null;
		}

		final int level = requests.level();
		@SuppressWarnings("unchecked")
		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
		final long[] gridPos = requests.gridPosition();
		final RawShard shard = codec.decode(existingReadData, gridPos).getData();

		boolean modified = false;
		boolean shardElementSetToNull = false;
		if ( level == 1 ) {
			// Base case, delete the blocks
			for (final DataBlockRequest<T> request : requests) {
				final long[] elementPos = request.position.relative(0);
				if (shard.getElementData(elementPos) != null) {
					shard.setElementData(null, elementPos);
					modified = true;
					shardElementSetToNull = true;
				}
			}
		} else { // level > 1
			final List<DataBlockRequests<T>> split = requests.split();
			for (final DataBlockRequests<T> subRequests : split) {
				final boolean writeFully = subRequests.coversShard();
				final long[] elementPos = subRequests.relativeGridPosition();
				final ReadData existingElementData = writeFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = deleteBlocksRecursive(existingElementData, subRequests);
				if (modifiedElementData != existingElementData) {
					shard.setElementData(modifiedElementData, elementPos);
					modified = true;
					shardElementSetToNull |= (modifiedElementData == null);
				}
			}
		}

		if (!modified) {
			// No nested shard or DataBlock was modified.
			// This shard remains unchanged.
			return existingReadData;
		}

		if (shardElementSetToNull) {
			// At least one DataBlock or nested shard was removed.
			// Check whether this shard becomes empty.
			if (shard.index().allElementsNull()) {
				// This shard is empty and should be removed.
				return null;
			}
		}

		return codec.encode(new RawShardDataBlock(gridPos, shard));
	}



	//
	// -- readShard -----------------------------------------------------------

	// NB: How to handle the dataset borders?
	//
	// N5 format uses truncated DataBlocks at the dataset border.
	//
	// For the Zarr format, when a truncated DataBlock is written, it is
	// padded with zero values to the default DataBlock size. This will
	// happen also when writing DataBlocks into a Shard.
	//
	// However, Zarr will not fill up a Shard with empty DataBlocks to pad
	// it to the default Shard size. Instead, these blocks will be missing
	// in the Shard index.
	//
	// When we write a full Shard as a "big DataBlock", what do we expect?
	//
	// For N5 format probably we expect the big DataBlock to be truncated at
	// the dataset border. (N5 format doesn't have shards yet, so we are
	// relatively free what to do here. But this would be consistent.)
	//
	// For Zarr format, we either expect
	//  * a big DataBlock that is the default Shard size, or
	//  * a big DataBlock that is truncated after the last DataBlock that is
	//    (partially) in the dataset borders (but truncated at multiple of
	//    default DataBlock size, so "slightly padded").
	//
	// I'm not sure which, so we handle both cases for now. In any case, we
	// do not want to write DataBlocks that are completely outside the
	// dataset (even if the "big DataBlock" covers this area.)
	//
	// This works for writing. For reading we'll have to decide what to return,
	// though... Potentially, there is no valid block at the border, so we
	// cannot determine where to put the border just from the data. We need to
	// rely on external input or heuristics.
	//
	// For now, we decided to always truncate the at the dataset border when we
	// read full shards. This will hopefully work where we need it, but it
	// introduces inconsistencies. There is a readShardInternal() implementation
	// which takes the expected shardSizeInPixels as an argument, so that we can
	// easily revisit and change this heuristic.

	@Override
	public DataBlock<T> readShard(
			final PositionValueAccess pva,
			final long[] shardGridPosition,
			final int level
	) throws N5IOException {

		if (level == 0) {
			return readBlock(pva, shardGridPosition);
		}

		final long[] shardPixelPos = grid.pixelPosition(shardGridPosition, level);
		final int[] defaultShardSize = grid.getBlockSize(level);
		final long[] datasetSize = grid.getDatasetSize();

		final int n = grid.numDimensions();
		final int[] shardSizeInPixels = new int[n];
		for (int d = 0; d < n; ++d) {
			shardSizeInPixels[d] = Math.min(defaultShardSize[d], (int) (datasetSize[d] - shardPixelPos[d]));
		}
		return readShardInternal(pva, shardGridPosition, shardSizeInPixels, level);
	}

	private DataBlock<T> readShardInternal(
			final PositionValueAccess pva,
			final long[] shardGridPosition,
			final int[] shardSizeInPixels, // expected size of this shard in pixels
			final int level
	) throws N5IOException {

		final int n = grid.numDimensions();
		final int[] defaultShardSize = grid.getBlockSize(level);
		for (int d = 0; d < n; d++) {
			if (shardSizeInPixels[d] > defaultShardSize[d]) {
				throw new IllegalArgumentException("Requested shard size is larger than the default shard size");
			}
		}

		// level-0 block-grid position of the min block in the shard
		final long[] gridMin = grid.absolutePosition(shardGridPosition, level, 0);

		// level-0 block-grid position of the max block in the shard that we need to read.
		// (the shard might go beyond the dataset border, and we don't need to read anything there)
		final long[] gridMax = new long[n];
		final long[] datasetSizeInBlocks = grid.getDatasetSizeInBlocks();
		final int[] blockSize = grid.getBlockSize(0);
		for (int d = 0; d < n; ++d) {
			final int shardSizeInBlocks = (shardSizeInPixels[d] + blockSize[d] - 1) / blockSize[d];
			final int gridSize = Math.min(shardSizeInBlocks, (int) (datasetSizeInBlocks[d] - gridMin[d]));
			gridMax[d] = gridMin[d] + gridSize - 1;
		}

		// read all blocks in (gridMin, gridMax) and filter out missing blocks
		final List<long[]> blockPositions = Region.gridPositions(gridMin, gridMax);
		final List<DataBlock<T>> blocks = readBlocks(pva, blockPositions)
				.stream().filter(Objects::nonNull).collect(Collectors.toList());
		if (blocks.isEmpty()) {
			return null;
		}

		// allocate shard and copy data from blocks
		final DataBlock<T> shard = DataBlockFactory.of(blocks.get(0).getData()).createDataBlock(shardSizeInPixels, shardGridPosition);
		final long[] shardPixelPos = grid.pixelPosition(shardGridPosition, level);
		final long[] blockPixelPos = new long[n];
		final int[] srcPos = new int[n];
		final int[] destPos = new int[n];
		final int[] size = new int[n];
		for (final DataBlock<T> block : blocks) {
			// copy block data that overlaps the shard
			grid.pixelPosition(block.getGridPosition(), 0, blockPixelPos);
			final int[] bsize = block.getSize();
			for (int d = 0; d < n; d++) {
				destPos[d] = (int) (blockPixelPos[d] - shardPixelPos[d]);
				size[d] = Math.min(bsize[d], shardSizeInPixels[d] - destPos[d]);
			}
			SubArrayCopy.copy(block.getData(), bsize, srcPos, shard.getData(), shardSizeInPixels, destPos, size);
		}
		return shard;
	}

	//
	// -- writeShard ----------------------------------------------------------

	@Override
	public void writeShard(
			final PositionValueAccess pva,
			final DataBlock<T> dataBlock,
			final int level
	) throws N5IOException {

		if (level == 0) {
			writeBlock(pva, dataBlock);
			return;
		}

		final T shardData = dataBlock.getData();
		final DataBlockFactory<T> blockFactory = DataBlockFactory.of(shardData);

		final int n = grid.numDimensions();
		final int[] blockSize = grid.getBlockSize(0); // size of a standard (non-truncated) DataBlock
		final long[] datasetBlockSize = grid.getDatasetSizeInBlocks();

		final long[] shardPixelMin = grid.pixelPosition(dataBlock.getGridPosition(), level);
		final int[] shardPixelSize = dataBlock.getSize();

		// the max block + 1 in the shard, if it isn't truncated by the dataset border
		final long[] shardBlockTo = new long[n];
		Arrays.setAll(shardBlockTo, d -> (shardPixelMin[d] + shardPixelSize[d] + blockSize[d] - 1) / blockSize[d]);

		// level 0 DataBlock positions of all DataBlocks we want to extract
		final long[] gridMin = grid.absolutePosition(dataBlock.getGridPosition(), level, 0);
		final long[] gridMax = new long[n];
		Arrays.setAll(gridMax, d -> Math.min(shardBlockTo[d], datasetBlockSize[d]) - 1);
		final List<long[]> blockPositions = Region.gridPositions(gridMin, gridMax);

		// Max pixel coordinates + 1, of the region we want to copy. This should
		// always be shardPixelMin + shardPixelSize, except at the dataset
		// border, where we truncate to the smallest multiple of blockSize still
		// overlapping the dataset.
		final long[] regionBound = new long[n];
		Arrays.setAll(regionBound, d -> Math.min(shardPixelMin[d] + shardPixelSize[d], datasetBlockSize[d] * blockSize[d]));

		final List<DataBlock<T>> blocks = new ArrayList<>(blockPositions.size());
		final int[] srcPos = new int[n];
		final int[] destPos = new int[n];
		final int[] destSize = new int[n];
		for ( long[] blockPos : blockPositions) {
			final long[] pixelMin = grid.pixelPosition(blockPos, 0);

			for (int d = 0; d < n; d++) {
				srcPos[d] = (int) (pixelMin[d] - shardPixelMin[d]);
				destSize[d] = Math.min(blockSize[d], (int) (regionBound[d] - pixelMin[d]));
			}

			// This extracting DataBlocks will not work if num_array_elements != num_block_elements.
			// But we'll deal with that later if it becomes a problem...
			final DataBlock<T> block = blockFactory.createDataBlock(destSize, blockPos);
			SubArrayCopy.copy(shardData, shardPixelSize, srcPos, block.getData(), destSize, destPos, destSize);
			blocks.add(block);
		}

		writeBlocks(pva, blocks);
	}

	//
	// -- helpers -------------------------------------------------------------

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
	 * A request to read or write a (level-0) DataBlock at a given {@link #position}.
	 * <p>
	 * <em>Write requests</em> are constructed with {@link #position} and {@link #block}.
	 * <p>
	 * <em>Read requests</em> are constructed with only a {@link #position}, and
	 * initially {@link #block block=null}. When the DataBlock is read, it will
	 * be put into {@link #block}.
	 * <p>
	 * {@code DataBlockRequest} are used for reading/writing a list of blocks
	 * with {@link #readBlocks} and {@link #writeBlocks}. The {@link #index}
	 * field is the position in the list of positions/blocks to read/write. For
	 * processing, requests are re-ordered such that all requests from the same
	 * (sub-)shard are grouped together. The {@link #index} field is used to
	 * re-establish the order of results (only important for {@link
	 * #readBlocks}).
	 *
	 * @param <T>
	 * 		type of the data contained in the DataBlock
	 */
	private static final class DataBlockRequest<T> {

		final NestedPosition position;
		final int index;
		DataBlock<T> block;

		// read request
		DataBlockRequest(final NestedPosition position, final int index) {
			this.position = position;
			this.index = index;
			this.block = null;
		}

		// write request
		DataBlockRequest(final NestedPosition position, final DataBlock<T> block) {
			this.position = position;
			this.index = -1;
			this.block = block;
		}

		@Override
		public String toString() {
			return "DataBlockRequest{position=" + position + ", index=" + index + '}';
		}
	}

	/**
	 * A list of {@code DataBlockRequest}, ordered by {@code NestedPosition}.
	 * All requests lie in the same shard at the given {@code level}.
	 * <p>
	 * {@code DataBlockRequests} should be constructed using {@link
	 * #createReadRequests} (for reading) or {@link #createWriteRequests} (for
	 * writing).
	 * <p>
	 * When recursing into nested shard levels, {@code DataBlockRequests} should
	 * be {@link #split} to partition into sub-{@code DataBlockRequests} that
	 * each cover one shard.
	 *
	 * @param <T>
	 * 		type of the data contained in the DataBlocks
	 */
	private static final class DataBlockRequests<T> implements Iterable<DataBlockRequest<T>> {

		private final NestedGrid grid;
		private final List<DataBlockRequest<T>> requests;
		private final int level;

		private DataBlockRequests(final List<DataBlockRequest<T>> requests, final int level, final NestedGrid grid) {
			this.requests = requests;
			this.level = level;
			this.grid = grid;
		}

		/**
		 * Returns a map of duplicate requests. Each pair of consecutive
		 * elements (A,B) of the list means that request A is a duplicate of
		 * request B. A has been removed from the {@code requests} list, and B
		 * remains in the {@code requests} list. After the (read) requests have
		 * been processed, elements corresponding to A can be added into the
		 * result list by using the {@code DataBlock} of B.
		 * <p>
		 * If {@code n} duplicates occur in {@code requests}, the resulting list
		 * will have {@code 2*n} elements.
		 */
		public List<DataBlockRequest<T>> removeDuplicates() {
			List<DataBlockRequest<T>> duplicates = new ArrayList<>();
			DataBlockRequest<T> previous = null;
			final ListIterator<DataBlockRequest<T>> iter = requests.listIterator();
			while (iter.hasNext()) {
				final DataBlockRequest<T> current = iter.next();
				if (previous != null) {
					if (previous.position.equals(current.position)) {
						iter.remove();
						duplicates.add(current);
						duplicates.add(previous);
						continue;
					}
				}
				previous = current;
			}
			return duplicates;
		}

		@Override
		public Iterator<DataBlockRequest<T>> iterator() {
			return requests.iterator();
		}

		/**
		 * All blocks contained in this {@code DataBlockRequests} are in the
		 * same shard at this nesting level.
		 * <p>
		 * Use {@link #split()} to partition into {@code DataBlockRequests} with
		 * nesting level {@link #level()}{@code -1}.
		 *
		 * @return nesting level
		 */
		public int level() {
			return level;
		}

		/**
		 * Position on the shard grid at the level of this DataBlockRequests
		 * (of the one shard containing all the requested blocks).
		 */
		public long[] gridPosition() {
			return position().absolute(level);
		}

		/**
		 * Relative grid position at the level of this DataBlockRequests,
		 * that is, relative offset within containing the (level+1) element.
		 */
		public long[] relativeGridPosition() {
			return position().relative(level);
		}

		private NestedPosition position() {
			if (requests.isEmpty())
				throw new IllegalArgumentException();
			return requests.get(0).position;
		}

		/**
		 * Split into sub-requests, grouping by same position at nesting level {@link #level()}{@code -1}.
		 */
		public List<DataBlockRequests<T>> split() {
			final int subLevel = level - 1;
			final List<DataBlockRequests<T>> subRequests = new ArrayList<>();
			for (int i = 0; i < requests.size(); ) {
				final long[] ilpos = requests.get(i).position.absolute(subLevel);
				int j = i + 1;
				for (; j < requests.size(); ++j) {
					final long[] jlpos = requests.get(j).position.absolute(subLevel);
					if (!Arrays.equals(ilpos, jlpos)) {
						break;
					}
				}
				subRequests.add(new DataBlockRequests<>(requests.subList(i, j), subLevel, grid));
				i = j;
			}
			return subRequests;
		}

		/**
		 * Returns {@code true} if this {@code DataBlockRequests} completely
		 * fills its containing shard at nesting level {@link #level()}.
		 * (This can be used to avoid reading a shard that will be completely
		 * overwritten).
		 * <p>
		 * Note that this method only works correctly if the requests list
		 * contains no duplicates. See {@link #removeDuplicates()}.
		 */
		public boolean coversShard() {
			final long[] gridMin = grid.absolutePosition(position().absolute(level), level, 0);
			final long[] datasetSize = grid.getDatasetSizeInBlocks(); // in units of DataBlocks
			final int[] defaultShardSize = grid.relativeToBaseBlockSize(level); // in units of DataBlocks
			int numElements = 1;
			for (int d = 0; d < defaultShardSize.length; d++) {
				numElements *= Math.min(defaultShardSize[d], (int) (datasetSize[d] - gridMin[d]));
			}
			return requests.size() >= numElements;// NB: It should never be (requests.size() > numElements), unless there are duplicate blocks.
		}

		/**
		 * Extract {@link DataBlockRequest#block DataBlock}s from the requests,
		 * in the order of {@link DataBlockRequest#index indices}.
		 * <p>
		 * (This is used in {@link #readBlocks} to collect DataBlocks in the
		 * requested order.)
 		 */
		public List<DataBlock<T>> blocks(final List<DataBlockRequest<T>> duplicates) {
			final int size = requests.size() + duplicates.size() / 2;
			final DataBlock<T>[] blocks = new DataBlock[size];
			requests.forEach(r -> blocks[r.index] = r.block);
			for (int i = 0; i < duplicates.size(); i += 2) {
				final DataBlockRequest<T> a = duplicates.get(i * 2);
				final DataBlockRequest<T> b = duplicates.get(i * 2 + 1);
				blocks[a.index] = b.block;
			}
			return Arrays.asList(blocks);
		}
	}

	/**
	 * Construct {@code DataBlockRequests} from a list of level-0 grid positions
	 * for reading.
	 * <p>
	 * The nesting level ot the returned {@code DataBlockRequests} is {@code
	 * grid.numLevels()}, that is level of the highest-order shard + 1. This
	 * implies that the requests are not guaranteed to be in the same shard (at
	 * any level. {@link DataBlockRequests#split() Splitting} the {@code
	 * DataBlockRequests} once will return a list of {@code DataBlockRequests}
	 * that each contain blocks from one highest-order shard.
	 */
	private DataBlockRequests<T> createReadRequests(final List<long[]> gridPositions) {
		final List<DataBlockRequest<T>> requests = new ArrayList<>(gridPositions.size());
		for (int i = 0; i < gridPositions.size(); i++) {
			final NestedPosition pos = grid.nestedPosition(gridPositions.get(i));
			requests.add(new DataBlockRequest<>(pos, i));
		}
		requests.sort(Comparator.comparing(r -> r.position));
		return new DataBlockRequests<>(requests, grid.numLevels(), grid);
	}

	/**
	 * Construct {@code DataBlockRequests} from a list of level-0 DataBlocks for
	 * writing.
	 * <p>
	 * The nesting level ot the returned {@code DataBlockRequests} is {@code
	 * grid.numLevels()}, that is level of the highest-order shard + 1. This
	 * implies that the requests are not guaranteed to be in the same shard (at
	 * any level. {@link DataBlockRequests#split() Splitting} the {@code
	 * DataBlockRequests} once will return a list of {@code DataBlockRequests}
	 * that each contain blocks from one highest-order shard.
	 */
	private DataBlockRequests<T> createWriteRequests(final List<DataBlock<T>> dataBlocks) {
		final List<DataBlockRequest<T>> requests = new ArrayList<>(dataBlocks.size());
		for (final DataBlock<T> dataBlock : dataBlocks) {
			final NestedPosition pos = grid.nestedPosition(dataBlock.getGridPosition());
			requests.add(new DataBlockRequest<>(pos, dataBlock));
		}
		requests.sort(Comparator.comparing(r -> r.position));
		return new DataBlockRequests<>(requests, grid.numLevels(), grid);
	}

	/**
	 * Factory for the standard {@code DataBlock<T>}, where {@code T} is an
	 * array type and the number of elements in a block corresponds to the
	 * {@link DataBlock#getSize()}.
	 * <p>
	 * This is used by {@link #readShard} and {@link #writeShard} which
	 * internally need to allocate new DataBlocks to split or merge a shard.
	 */
	private interface DataBlockFactory<T> {

		DataBlock<T> createDataBlock(final int[] blockSize, final long[] gridPosition);

		@SuppressWarnings("unchecked")
		static <T> DataBlockFactory<T> of(T array) {
			if (array instanceof byte[]) {
				return (size, pos) -> (DataBlock<T>) new ByteArrayDataBlock(size, pos, new byte[DataBlock.getNumElements(size)]);
			} else if (array instanceof short[]) {
				return (size, pos) -> (DataBlock<T>) new ShortArrayDataBlock(size, pos, new short[DataBlock.getNumElements(size)]);
			} else if (array instanceof int[]) {
				return (size, pos) -> (DataBlock<T>) new IntArrayDataBlock(size, pos, new int[DataBlock.getNumElements(size)]);
			} else if (array instanceof long[]) {
				return (size, pos) -> (DataBlock<T>) new LongArrayDataBlock(size, pos, new long[DataBlock.getNumElements(size)]);
			} else if (array instanceof float[]) {
				return (size, pos) -> (DataBlock<T>) new FloatArrayDataBlock(size, pos, new float[DataBlock.getNumElements(size)]);
			} else if (array instanceof double[]) {
				return (size, pos) -> (DataBlock<T>) new DoubleArrayDataBlock(size, pos, new double[DataBlock.getNumElements(size)]);
			} else if (array instanceof String[]) {
				return (size, pos) -> (DataBlock<T>) new StringDataBlock(size, pos, new String[DataBlock.getNumElements(size)]);
			} else {
				throw new IllegalArgumentException("unsupported array type: " + array.getClass().getSimpleName());
			}
		}
	}
}
