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
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
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

	@Override
	public DataBlock<T> readChunk(final PositionValueAccess pva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = grid.nestedPosition(gridPosition);
		try (final VolatileReadData readData = pva.get(position.key())) {
			return readChunkRecursive(readData, position, grid.numLevels() - 1);
		} catch (N5NoSuchKeyException ignored) {
			return null;
		}
	}

	private DataBlock<T> readChunkRecursive(
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
			return readChunkRecursive(shard.getElementData(position.relative(level - 1)), position, level - 1);
		}
	}

	@Override
	public List<DataBlock<T>> readChunks(final PositionValueAccess pva, final List<long[]> gridPositions) throws N5IOException {

		// for non-sharded datasets, just read the chunks individually
		if (grid.numLevels() == 1) {
			return gridPositions.stream().map(pos -> readChunk(pva, pos)).collect(Collectors.toList());
		}

		// Create a list of ChunkRequests and sort it such that requests
		// from the same (nested) shard are grouped contiguously.
		final ChunkRequests<T> requests = createReadRequests(gridPositions);
		final List<ChunkRequest<T>> duplicates = requests.removeDuplicates();

		final List<ChunkRequests<T>> split = requests.split();
		for (final ChunkRequests<T> subRequests : split) {
			final long[] key = subRequests.relativeGridPosition();
			try (final VolatileReadData readData = pva.get(key)) {
				readChunksRecursive(readData, subRequests);
			} catch (N5NoSuchKeyException ignored) {
				// the key didn't exist (as we found out when lazy-reading the index).
				// we don't have to do anything: all subRequest blocks remain null.
				// on to the next shard.
			}
		}

		return requests.chunks(duplicates);
	}

	/**
	 * Bulk Read operation on a shard.
	 *
	 * @param readData for the corresponding shard
	 * @param requests for chunks within the shard to be read
	 */
	private void readChunksRecursive(
			final ReadData readData,
			final ChunkRequests<T> requests
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
			//Base case; read the chunks

			// TODO: collect all the elementPos that we will need and prefetch
			//       Probably best to add a prefetch method to RawShard?

			for (final ChunkRequest<T> request : requests) {
				final long[] elementPos = request.position.relative(0);
				final ReadData elementData = shard.getElementData(elementPos);
				request.chunk = readChunkRecursive(elementData, request.position, 0);
			}
		} else { // level > 1
			final List<ChunkRequests<T>> split = requests.split();
			for (final ChunkRequests<T> subRequests : split) {
				final long[] subShardPosition = subRequests.relativeGridPosition();
				final ReadData elementData = shard.getElementData(subShardPosition);
				readChunksRecursive(elementData, subRequests);
			}
		}
	}

	@Override
	public void writeChunk(final PositionValueAccess pva, final DataBlock<T> chunk) throws N5IOException {

		if (grid.numLevels() == 1) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			pva.set(chunk.getGridPosition(), codec.encode(chunk));
		} else {
			final NestedPosition position = grid.nestedPosition(chunk.getGridPosition());
			final long[] key = position.key();
			final ReadData modifiedData;
			try (final VolatileReadData existingData = pva.get(key)) {
				modifiedData = writeChunkRecursive(existingData, chunk, position, grid.numLevels() - 1);
				// Here, we are about to write the shard data, but with the new block modified.
				// Need to make sure that the read operations happen now before pva.set acquires a write lock
				modifiedData.materialize();
			}
			pva.set(key, modifiedData);
		}
	}

	private ReadData writeChunkRecursive(
			final ReadData existingReadData,
			final DataBlock<T> chunk,
			final NestedPosition position,
			final int level) {
		if (level == 0) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			return codec.encode(chunk);
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = getRawShard(existingReadData, codec, gridPos, level);
			final long[] elementPos = position.relative(level - 1);
			final ReadData existingElementData = (level == 1)
					? null // if level == 1, we don't need to extract the nested (DataBlock<T>) ReadData because it will be overridden anyway
					: shard.getElementData(elementPos);
			final ReadData modifiedElementData = writeChunkRecursive(existingElementData, chunk, position, level - 1);
			shard.setElementData(modifiedElementData, elementPos);
			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	@Override
	public void writeChunks(final PositionValueAccess pva, final List<DataBlock<T>> chunks) throws N5IOException {

		if (grid.numLevels() == 1) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			chunks.forEach(chunk -> pva.set(chunk.getGridPosition(), codec.encode(chunk)));
		} else {
			// Create a list of ChunkRequests, sorted such that requests from
			// the same (nested) shard are grouped contiguously.
			final ChunkRequests<T> requests = createWriteRequests(chunks);
			requests.removeDuplicates();
			final List<ChunkRequests<T>> split = requests.split();
			for (final ChunkRequests<T> subRequests : split) {
				final boolean writeFully = subRequests.coversShard();
				final long[] shardKey = subRequests.relativeGridPosition();
				final ReadData modifiedData;
				try (final VolatileReadData existingData = writeFully ? null : pva.get(shardKey)) {
					modifiedData = writeChunksRecursive(existingData, subRequests);
					// Here, we are about to write the shard data, but with the new blocks modified.
					// Need to make sure that the read operations happen now before pva.set acquires a write lock
					modifiedData.materialize();
				}
				pva.set(shardKey, modifiedData);
			}
		}
	}

	/**
	 * Bulk Write operation on a shard.
	 *
	 * @param existingReadData encoded existing shard data (to decode and partially override)
	 * @param requests for chunks within the shard to be written
	 */
	private ReadData writeChunksRecursive(
			final ReadData existingReadData, // may be null
			final ChunkRequests<T> requests
	) {
		assert !requests.requests.isEmpty();
		assert requests.level > 0;

		final boolean writeFully = existingReadData == null;

		final int level = requests.level();
		@SuppressWarnings("unchecked")
		final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
		final long[] gridPos = requests.gridPosition();
		final RawShard shard = getRawShard(existingReadData, codec, gridPos, level);

		if ( level == 1 ) {
			// Base case, write the blocks
			for (final ChunkRequest<T> request : requests) {
				final ReadData elementData = writeChunkRecursive(null, request.chunk, request.position, 0);
				final long[] elementPos = request.position.relative(0);
				shard.setElementData(elementData, elementPos);
			}
		} else { // level > 1
			final List<ChunkRequests<T>> split = requests.split();
			for (final ChunkRequests<T> subRequests : split) {
				final boolean nestedWriteFully = writeFully || subRequests.coversShard();
				final long[] elementPos = subRequests.relativeGridPosition();
				final ReadData existingElementData = nestedWriteFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = writeChunksRecursive(existingElementData, subRequests);
				shard.setElementData(modifiedElementData, elementPos);
			}
		}

		return codec.encode(new RawShardDataBlock(gridPos, shard));
	}

	@Override
	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully
	) throws N5IOException {

		final Region region = new Region(min, size, grid);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
			final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
			final ReadData modifiedData;
			try (final VolatileReadData existingData = nestedWriteFully ? null : pva.get(key)) {
				modifiedData = writeRegionRecursive(existingData, region, chunkSupplier, pos);
				// Here, we are about to write the shard data, but with the new shard modified.
				// Need to make sure that the read operations happen now before pva.set acquires a write lock
				if (existingData != null && modifiedData != null) {
					modifiedData.materialize();
				}
			}
			pva.set(key, modifiedData);
		}
	}

	@Override
	public void writeRegion(
			final PositionValueAccess pva,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		final Region region = new Region(min, size, grid);

		for (long[] key : Region.gridPositions(region.minPos().key(), region.maxPos().key())) {
			exec.submit(() -> {
				final NestedPosition pos = grid.nestedPosition(key, grid.numLevels() - 1);
				final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
				final ReadData modifiedData;
				try (final VolatileReadData existingData = nestedWriteFully ? null : pva.get(key)) {
					modifiedData = writeRegionRecursive(existingData, region, chunkSupplier, pos);
					// Here, we are about to write the shard data, but with the new block modified.
					// Need to make sure that the read operations happen now before pva.set acquires a write lock
					if (existingData != null && modifiedData != null) {
						modifiedData.materialize();
					}
				}
				pva.set(key, modifiedData);
			});
		}
	}

	private ReadData writeRegionRecursive(
			final ReadData existingReadData, // may be null
			final Region region,
			final DataBlockSupplier<T> chunkSupplier,
			final NestedPosition position
	) {
		final boolean writeFully = existingReadData == null;
		final int level = position.level();
		if ( level == 0 ) {

			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			final long[] gridPosition = position.absolute(0);

			// If the DataBlock is not fully contained in the region, we will
			// get existingReadData != null. In that case, we try to decode the
			// existing DataBlock and pass it to the BlockSupplier for modification.
			// (This might fail with N5NoSuchKeyException if existingReadData
			// lazily points to non-existent data.)
			DataBlock<T> existingChunk = null;
			if (existingReadData != null) {
				try {
					existingChunk = codec.decode(existingReadData, gridPosition);
				} catch (N5NoSuchKeyException ignored) {
				}
			}
			final DataBlock<T> chunk = chunkSupplier.get(gridPosition, existingChunk);

			// null chunks may be provided when they contain only the fill value
			// and only non-empty chunks should be written, for example
			if (chunk == null)
				return null;

			return codec.encode(chunk);
		} else {

			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = getRawShard(existingReadData, codec, gridPos, level);
			for (NestedPosition pos : region.containedNestedPositions(position)) {
				final boolean nestedWriteFully = writeFully || region.fullyContains(pos);
				final long[] elementPos = pos.relative();
				final ReadData existingElementData = nestedWriteFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = writeRegionRecursive(existingElementData, region, chunkSupplier, pos);
				shard.setElementData(modifiedElementData, elementPos);
			}

			// do not write empty shards
			if (shard.isEmpty())
				return null;

			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	//
	// -- deleteChunk ---------------------------------------------------------

	@Override
	public boolean deleteChunk(final PositionValueAccess pva, final long[] gridPosition) throws N5IOException {
		if (grid.numLevels() == 1) {
			// for non-sharded dataset, don't bother getting the value, just remove the key.
			return pva.remove(gridPosition);
		} else {
			final NestedPosition position = grid.nestedPosition(gridPosition);
			final long[] key = position.key();
			final ReadData modifiedData;
			try (final VolatileReadData existingData = pva.get(key)) {
				modifiedData = deleteChunkRecursive(existingData, position, grid.numLevels() - 1);
				if (modifiedData == existingData) {
					// nothing changed, the blocks we wanted to delete didn't exist anyway
					return false;
				} else if (modifiedData != null) {
					// Here, we are about to write the shard data, but without the chunk to be deleted.
					// Need to make sure that the read operations happen now before pva.set acquires a write lock
					modifiedData.materialize();
				}
			} catch (final N5NoSuchKeyException e) {
				// the key didn't exist (as we found out when lazy-reading the index)
				// so nothing changed, the chunks we wanted to delete didn't exist anyway
				return false;
			}
			if (modifiedData == null) {
				return pva.remove(key);
			} else {
				pva.set(key, modifiedData);
				return true;
			}
		}
	}

	private ReadData deleteChunkRecursive(
			final ReadData existingReadData,
			final NestedPosition position,
			final int level) throws N5NoSuchKeyException {
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
				// The chunk (or the whole nested shard containing it) does not exist.
				// This shard remains unchanged.
				return existingReadData;
			} else {
				final ReadData modifiedElementData = deleteChunkRecursive(existingElementData, position, level - 1);
				if (modifiedElementData == existingElementData) {
					// The nested shard was not modified.
					// This shard remains unchanged.
					return existingReadData;
				}
				shard.setElementData(modifiedElementData, elementPos);
				if (modifiedElementData == null) {
					// The chunk or nested shard was removed.
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
	// -- deleteChunks --------------------------------------------------------

	@Override
	public boolean deleteChunks(final PositionValueAccess pva, final List<long[]> gridPositions) throws N5IOException {

		// for non-sharded datasets, just delete the chunks individually
		if (grid.numLevels() == 1) {
			boolean deleted = false;
			for (long[] pos : gridPositions) {
				deleted |= pva.remove(pos);
			}
			return deleted;
		} else {

			// Create a list of ChunkRequests and sort it such that requests
			// from the same (nested) shard are grouped contiguously.
			// Despite the name, createReadRequests() works for delete requests as well ...
			final ChunkRequests<T> requests = createReadRequests(gridPositions);
			requests.removeDuplicates();

			boolean deleted = false;
			final List<ChunkRequests<T>> split = requests.split();
			for (final ChunkRequests<T> subRequests : split) {
				final boolean writeFully = subRequests.coversShard();
				final long[] key = subRequests.relativeGridPosition();
				final ReadData modifiedData;
				try (final VolatileReadData existingData = writeFully ? null : pva.get(key)) {
					modifiedData = deleteChunksRecursive(existingData, subRequests);;
					if (modifiedData == existingData) {
						// nothing changed, the chunks we wanted to delete didn't exist anyway
						continue;
					} else if (existingData != null && modifiedData != null) {
						// Here, we are about to write the shard data, but without the chunk to be deleted.
						// Need to make sure that the read operations happen now before pva.set acquires a write lock
						modifiedData.materialize();
					}
				} catch (final N5NoSuchKeyException e) {
					// the key didn't exist (as we found out when lazy-reading the index)
					// so nothing changed, the chunks we wanted to delete didn't exist anyway
					continue;
				}
				if (modifiedData == null) {
					deleted |= pva.remove(key);
				} else {
					pva.set(key, modifiedData);
					deleted = true;
				}
			}
			return deleted;
		}
	}

	/**
	 * Bulk Delete operation on a shard.
	 *
	 * @param existingReadData encoded existing shard data (to decode and partially override)
	 * @param requests for chunks within the shard to be deleted
	 */
	private ReadData deleteChunksRecursive(
			final ReadData existingReadData, // may be null
			final ChunkRequests<T> requests
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
			// Base case, delete the chunks
			for (final ChunkRequest<T> request : requests) {
				final long[] elementPos = request.position.relative(0);
				if (shard.getElementData(elementPos) != null) {
					shard.setElementData(null, elementPos);
					modified = true;
					shardElementSetToNull = true;
				}
			}
		} else { // level > 1
			final List<ChunkRequests<T>> split = requests.split();
			for (final ChunkRequests<T> subRequests : split) {
				final boolean writeFully = subRequests.coversShard();
				final long[] elementPos = subRequests.relativeGridPosition();
				final ReadData existingElementData = writeFully ? null : shard.getElementData(elementPos);
				final ReadData modifiedElementData = deleteChunksRecursive(existingElementData, subRequests);
				if (modifiedElementData != existingElementData) {
					shard.setElementData(modifiedElementData, elementPos);
					modified = true;
					shardElementSetToNull |= (modifiedElementData == null);
				}
			}
		}

		if (!modified) {
			// No nested shard or chunk was modified.
			// This shard remains unchanged.
			return existingReadData;
		}

		if (shardElementSetToNull) {
			// At least one chunk or nested shard was removed.
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
	// This works for writing. For reading, we'll have to decide what to return,
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
	public DataBlock<T> readBlock(
			final PositionValueAccess pva,
			final long[] shardGridPosition,
			final int level
	) throws N5IOException {

		if (level == 0) {
			return readChunk(pva, shardGridPosition);
		}

		final long[] shardPixelPos = grid.pixelPosition(shardGridPosition, level);
		final int[] defaultShardSize = grid.getBlockSize(level);
		final long[] datasetSize = grid.getDatasetSize();

		final int n = grid.numDimensions();
		final int[] shardSizeInPixels = new int[n];
		for (int d = 0; d < n; ++d) {
			shardSizeInPixels[d] = Math.min(defaultShardSize[d], (int) (datasetSize[d] - shardPixelPos[d]));
		}
		return readBlockInternal(pva, shardGridPosition, shardSizeInPixels, level);
	}

	private DataBlock<T> readBlockInternal(
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

		// level-0 block-grid position of the min chunk in the shard
		final long[] gridMin = grid.absolutePosition(shardGridPosition, level, 0);

		// level-0 block-grid position of the max chunk in the shard that we need to read.
		// (the shard might go beyond the dataset border, and we don't need to read anything there)
		final long[] gridMax = new long[n];
		final long[] datasetSizeInChunks = grid.getDatasetSizeInChunks();
		final int[] chunkSize = grid.getBlockSize(0);
		for (int d = 0; d < n; ++d) {
			final int shardSizeInChunks = (shardSizeInPixels[d] + chunkSize[d] - 1) / chunkSize[d];
			final int gridSize = Math.min(shardSizeInChunks, (int) (datasetSizeInChunks[d] - gridMin[d]));
			gridMax[d] = gridMin[d] + gridSize - 1;
		}

		// read all chunks in (gridMin, gridMax) and filter out missing chunks
		final List<long[]> chunkPositions = Region.gridPositions(gridMin, gridMax);
		final List<DataBlock<T>> chunks = readChunks(pva, chunkPositions)
				.stream().filter(Objects::nonNull).collect(Collectors.toList());
		if (chunks.isEmpty()) {
			return null;
		}

		// allocate shard and copy data from chunks
		final DataBlock<T> shard = DataBlockFactory.of(chunks.get(0).getData()).createDataBlock(shardSizeInPixels, shardGridPosition);
		final long[] shardPixelPos = grid.pixelPosition(shardGridPosition, level);
		final long[] chunkPixelPos = new long[n];
		final int[] srcPos = new int[n];
		final int[] destPos = new int[n];
		final int[] size = new int[n];
		for (final DataBlock<T> chunk : chunks) {
			// copy chunk data that overlaps the shard
			grid.pixelPosition(chunk.getGridPosition(), 0, chunkPixelPos);
			final int[] bsize = chunk.getSize();
			for (int d = 0; d < n; d++) {
				destPos[d] = (int) (chunkPixelPos[d] - shardPixelPos[d]);
				size[d] = Math.min(bsize[d], shardSizeInPixels[d] - destPos[d]);
			}
			SubArrayCopy.copy(chunk.getData(), bsize, srcPos, shard.getData(), shardSizeInPixels, destPos, size);
		}
		return shard;
	}

	//
	// -- writeBlock ----------------------------------------------------------

	@Override
	public void writeBlock(
			final PositionValueAccess pva,
			final DataBlock<T> dataBlock,
			final int level
	) throws N5IOException {

		if (level == 0) {
			writeChunk(pva, dataBlock);
			return;
		}

		final T shardData = dataBlock.getData();
		final DataBlockFactory<T> blockFactory = DataBlockFactory.of(shardData);

		final int n = grid.numDimensions();
		final int[] chunkSize = grid.getBlockSize(0); // size of a standard (non-truncated) chunk
		final long[] datasetChunkSize = grid.getDatasetSizeInChunks();

		final long[] shardPixelMin = grid.pixelPosition(dataBlock.getGridPosition(), level);
		final int[] shardPixelSize = dataBlock.getSize();

		// the max chunk + 1 in the shard, if it isn't truncated by the dataset border
		final long[] shardChunkTo = new long[n];
		Arrays.setAll(shardChunkTo, d -> (shardPixelMin[d] + shardPixelSize[d] + chunkSize[d] - 1) / chunkSize[d]);

		// level 0 grid positions of all chunks we want to extract
		final long[] gridMin = grid.absolutePosition(dataBlock.getGridPosition(), level, 0);
		final long[] gridMax = new long[n];
		Arrays.setAll(gridMax, d -> Math.min(shardChunkTo[d], datasetChunkSize[d]) - 1);
		final List<long[]> chunkPositions = Region.gridPositions(gridMin, gridMax);

		// Max pixel coordinates + 1, of the region we want to copy. This should
		// always be shardPixelMin + shardPixelSize, except at the dataset
		// border, where we truncate to the smallest multiple of chunkSize still
		// overlapping the dataset.
		final long[] regionBound = new long[n];
		Arrays.setAll(regionBound, d -> Math.min(shardPixelMin[d] + shardPixelSize[d], datasetChunkSize[d] * chunkSize[d]));

		final List<DataBlock<T>> chunks = new ArrayList<>(chunkPositions.size());
		final int[] srcPos = new int[n];
		final int[] destPos = new int[n];
		final int[] destSize = new int[n];
		for ( long[] chunkPos : chunkPositions) {
			final long[] pixelMin = grid.pixelPosition(chunkPos, 0);

			for (int d = 0; d < n; d++) {
				srcPos[d] = (int) (pixelMin[d] - shardPixelMin[d]);
				destSize[d] = Math.min(chunkSize[d], (int) (regionBound[d] - pixelMin[d]));
			}

			// This extracting chunks will not work if num_array_elements != num_block_elements.
			// But we'll deal with that later if it becomes a problem...
			final DataBlock<T> chunk = blockFactory.createDataBlock(destSize, chunkPos);
			SubArrayCopy.copy(shardData, shardPixelSize, srcPos, chunk.getData(), destSize, destPos, destSize);
			chunks.add(chunk);
		}

		writeChunks(pva, chunks);
	}

	//
	// -- helpers -------------------------------------------------------------

	/**
	 * If {@code existingReadData != null} try to decode it into a RawShard.
	 * Otherwise, or if this fails because we find that {@code existingReadData}
	 * lazily points to non-existent data, return a new empty RawShard.
	 *
	 * @param existingReadData data to decode or null
	 * @param codec shard codec
	 * @param gridPos position of the shard on the shard grid of the given level
	 * @param level level of the shard
	 * @return the decode shard (or a new empty shard)
	 */
	private RawShard getRawShard(
			final ReadData existingReadData,
			final BlockCodec<RawShard> codec,
			final long[] gridPos,
			final int level) {
		if (existingReadData != null) {
			try {
				return codec.decode(existingReadData, gridPos).getData();
			} catch (N5NoSuchKeyException ignored) {
			}
		}
		return new RawShard(grid.relativeBlockSize(level));
	}

	/**
	 * A request to read or write a chunk (level-0 DataBlock) at a given {@link #position}.
	 * <p>
	 * <em>Write requests</em> are constructed with {@link #position} and {@link #chunk}.
	 * <p>
	 * <em>Read requests</em> are constructed with only a {@link #position}, and
	 * initially {@link #chunk chunk=null}. When the DataBlock is read, it will
	 * be put into {@link #chunk}.
	 * <p>
	 * {@code ChunkRequest} are used for reading/writing a list of chunks
	 * with {@link #readChunks} and {@link #writeChunks}. The {@link #index}
	 * field is the position in the list of positions/chunks to read/write. For
	 * processing, requests are re-ordered such that all requests from the same
	 * (sub-)shard are grouped together. The {@link #index} field is used to
	 * re-establish the order of results (only important for {@link #readChunks}).
	 *
	 * @param <T>
	 * 		type of the data contained in the DataBlock
	 */
	private static final class ChunkRequest<T> {

		final NestedPosition position;
		final int index;
		DataBlock<T> chunk;

		// read request
		ChunkRequest(final NestedPosition position, final int index) {
			this.position = position;
			this.index = index;
			this.chunk = null;
		}

		// write request
		ChunkRequest(final NestedPosition position, final DataBlock<T> chunk) {
			this.position = position;
			this.index = -1;
			this.chunk = chunk;
		}

		@Override
		public String toString() {
			return "ChunkRequest{position=" + position + ", index=" + index + '}';
		}
	}

	/**
	 * A list of {@code ChunkRequest}, ordered by {@code NestedPosition}.
	 * All requests lie in the same shard at the given {@code level}.
	 * <p>
	 * {@code ChunkRequests} should be constructed using {@link
	 * #createReadRequests} (for reading) or {@link #createWriteRequests} (for
	 * writing).
	 * <p>
	 * When recursing into nested shard levels, {@code ChunkRequests} should
	 * be {@link #split} to partition into sub-{@code ChunkRequests} that
	 * each cover one shard.
	 *
	 * @param <T>
	 * 		type of the data contained in the DataBlocks
	 */
	private static final class ChunkRequests<T> implements Iterable<ChunkRequest<T>> {

		private final NestedGrid grid;
		private final List<ChunkRequest<T>> requests;
		private final int level;

		private ChunkRequests(final List<ChunkRequest<T>> requests, final int level, final NestedGrid grid) {
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
		public List<ChunkRequest<T>> removeDuplicates() {
			List<ChunkRequest<T>> duplicates = new ArrayList<>();
			ChunkRequest<T> previous = null;
			final ListIterator<ChunkRequest<T>> iter = requests.listIterator();
			while (iter.hasNext()) {
				final ChunkRequest<T> current = iter.next();
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
		public Iterator<ChunkRequest<T>> iterator() {
			return requests.iterator();
		}

		/**
		 * All chunks contained in this {@code ChunkRequests} are in the
		 * same shard at this nesting level.
		 * <p>
		 * Use {@link #split()} to partition into {@code ChunkRequests} with
		 * nesting level {@link #level()}{@code -1}.
		 *
		 * @return nesting level
		 */
		public int level() {
			return level;
		}

		/**
		 * Position on the shard grid at the level of this ChunkRequests
		 * (of the one shard containing all the requested blocks).
		 */
		public long[] gridPosition() {
			return position().absolute(level);
		}

		/**
		 * Relative grid position at the level of this ChunkRequests,
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
		public List<ChunkRequests<T>> split() {
			final int subLevel = level - 1;
			final List<ChunkRequests<T>> subRequests = new ArrayList<>();
			for (int i = 0; i < requests.size(); ) {
				final long[] ilpos = requests.get(i).position.absolute(subLevel);
				int j = i + 1;
				for (; j < requests.size(); ++j) {
					final long[] jlpos = requests.get(j).position.absolute(subLevel);
					if (!Arrays.equals(ilpos, jlpos)) {
						break;
					}
				}
				subRequests.add(new ChunkRequests<>(requests.subList(i, j), subLevel, grid));
				i = j;
			}
			return subRequests;
		}

		/**
		 * Returns {@code true} if this {@code ChunkRequests} completely
		 * fills its containing shard at nesting level {@link #level()}.
		 * (This can be used to avoid reading a shard that will be completely
		 * overwritten).
		 * <p>
		 * Note that this method only works correctly if the requests list
		 * contains no duplicates. See {@link #removeDuplicates()}.
		 */
		public boolean coversShard() {
			final long[] gridMin = grid.absolutePosition(position().absolute(level), level, 0);
			final long[] datasetSize = grid.getDatasetSizeInChunks(); // in units of DataBlocks
			final int[] defaultShardSize = grid.relativeToBaseBlockSize(level); // in units of DataBlocks
			int numElements = 1;
			for (int d = 0; d < defaultShardSize.length; d++) {
				numElements *= Math.min(defaultShardSize[d], (int) (datasetSize[d] - gridMin[d]));
			}
			return requests.size() >= numElements;// NB: It should never be (requests.size() > numElements), unless there are duplicate blocks.
		}

		/**
		 * Extract {@link ChunkRequest#chunk chunk}s from the requests,
		 * in the order of {@link ChunkRequest#index indices}.
		 * <p>
		 * (This is used in {@link #readChunks} to collect chunks in the
		 * requested order.)
 		 */
		public List<DataBlock<T>> chunks(final List<ChunkRequest<T>> duplicates) {
			final int size = requests.size() + duplicates.size() / 2;
			final DataBlock<T>[] blocks = new DataBlock[size];
			requests.forEach(r -> blocks[r.index] = r.chunk);
			for (int i = 0; i < duplicates.size(); i += 2) {
				final ChunkRequest<T> a = duplicates.get(i * 2);
				final ChunkRequest<T> b = duplicates.get(i * 2 + 1);
				blocks[a.index] = b.chunk;
			}
			return Arrays.asList(blocks);
		}
	}

	/**
	 * Construct {@code ChunkRequests} from a list of level-0 grid positions
	 * for reading.
	 * <p>
	 * The nesting level ot the returned {@code ChunkRequests} is {@code
	 * grid.numLevels()}, that is level of the highest-order shard + 1. This
	 * implies that the requests are not guaranteed to be in the same shard (at
	 * any level. {@link ChunkRequests#split() Splitting} the {@code
	 * ChunkRequests} once will return a list of {@code ChunkRequests}
	 * that each contain chunks from one highest-order shard.
	 */
	private ChunkRequests<T> createReadRequests(final List<long[]> gridPositions) {
		final List<ChunkRequest<T>> requests = new ArrayList<>(gridPositions.size());
		for (int i = 0; i < gridPositions.size(); i++) {
			final NestedPosition pos = grid.nestedPosition(gridPositions.get(i));
			requests.add(new ChunkRequest<>(pos, i));
		}
		requests.sort(Comparator.comparing(r -> r.position));
		return new ChunkRequests<>(requests, grid.numLevels(), grid);
	}

	/**
	 * Construct {@code ChunkRequests} from a list of chunks (level-0
	 * DataBlocks) for writing.
	 * <p>
	 * The nesting level ot the returned {@code ChunkRequests} is {@code
	 * grid.numLevels()}, that is level of the highest-order shard + 1. This
	 * implies that the requests are not guaranteed to be in the same shard (at
	 * any level. {@link ChunkRequests#split() Splitting} the {@code
	 * ChunkRequests} once will return a list of {@code ChunkRequests}
	 * that each contain chunks from one highest-order shard.
	 */
	private ChunkRequests<T> createWriteRequests(final List<DataBlock<T>> dataBlocks) {
		final List<ChunkRequest<T>> requests = new ArrayList<>(dataBlocks.size());
		for (final DataBlock<T> dataBlock : dataBlocks) {
			final NestedPosition pos = grid.nestedPosition(dataBlock.getGridPosition());
			requests.add(new ChunkRequest<>(pos, dataBlock));
		}
		requests.sort(Comparator.comparing(r -> r.position));
		return new ChunkRequests<>(requests, grid.numLevels(), grid);
	}

	/**
	 * Factory for the standard {@code DataBlock<T>}, where {@code T} is an
	 * array type and the number of elements in a block corresponds to the
	 * {@link DataBlock#getSize()}.
	 * <p>
	 * This is used by {@link #readBlock} and {@link #writeBlock} which
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
