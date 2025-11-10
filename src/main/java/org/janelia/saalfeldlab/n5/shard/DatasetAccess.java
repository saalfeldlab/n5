package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedPosition;

/**
 * Wrap an instantiated DataBlock/shard codec hierarchy to implement (single and
 * batch) DataBlock read/write methods.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public interface DatasetAccess<T> {

	DataBlock<T> readBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;

	void writeBlock(PositionValueAccess kva, DataBlock<T> dataBlock) throws N5IOException;

	boolean deleteBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;


	List<DataBlock<T>> readBlocks(PositionValueAccess kva, List<long[]> positions) throws N5IOException;

	void writeBlocks(PositionValueAccess kva, List<DataBlock<T>> blocks) throws N5IOException;


	/**
	 *
	 * @param pva
	 * @param min
	 * 		min pixel coordinate of region to write
	 * @param size
	 * 		size in pixels of region to write
	 * @param blocks
	 * 		is asked to create blocks within the given region
	 * @param datasetDimensions
	 * @param writeFully
	 * 		if false, merge existing data in shards/blocks that overlap the region boundary. if true, override everything.
	 *
	 * @throws N5IOException
	 */
	void writeRegion(
			PositionValueAccess pva,
			long[] min,
			long[] size,
			DataBlockSupplier<T> blocks,
			long[] datasetDimensions,
			boolean writeFully
	) throws N5IOException;

	/**
	 *
	 * @param pva
	 * @param min
	 * 		min pixel coordinate of region to write
	 * @param size
	 * 		size in pixels of region to write
	 * @param blocks
	 * 		is asked to create blocks within the given region. must be thread-safe.
	 * @param datasetDimensions
	 * @param writeFully
	 * 		if false, merge existing data in shards/blocks that overlap the region boundary. if true, override everything.
	 * @param exec
	 * 		used to parallelize over blocks and shards
	 *
	 * @throws N5Exception
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	void writeRegion(
			PositionValueAccess pva,
			long[] min,
			long[] size,
			DataBlockSupplier<T> blocks,
			long[] datasetDimensions,
			boolean writeFully,
			ExecutorService exec
	) throws N5Exception, InterruptedException, ExecutionException;


	NestedGrid getGrid();

	/**
	 * Sort a list of {@link NestedPosition}s by their parent level {@link NestedPosition}.
	 * nestedPositions are grouped at `outerLevel`.
	 * If {@code NestedPosition.level()} level is already equivalent to {@code NestedGrid.numLevels() - 1}, nestedPositions is returned.
	 *
	 * @param grid to sort the blocky by
	 * @param innerPositions to group per shard.
	 * @param outerLevel of the outerLevel shard position to group by. must be in range {@code [1, NestedGrid.numLevels() - 1]}
	 * @return map of outerLevel shard positions to inner level block positions
	 */
	static <T extends NestedPosition> Collection<List<T>> groupInnerPositions(final NestedGrid grid, final List<T> innerPositions, final int outerLevel)  {

		if (outerLevel < 1 || outerLevel >= grid.numLevels())
			throw new IllegalArgumentException("outerLevel must be in range [1, grid.numLevels() - 1]");

		if (innerPositions.isEmpty())
			return Collections.emptyList();

		final TreeMap<NestedPosition, List<T>> blocksPerShard = new TreeMap<>();
		for (T nestedPosition : innerPositions) {
			final NestedPosition outerNestedPosition;
			if (nestedPosition.level() == outerLevel)
				outerNestedPosition = nestedPosition;
			else
				outerNestedPosition = grid.nestedPosition(nestedPosition.absolute(outerLevel), outerLevel);


			final List<T> blocks = blocksPerShard.computeIfAbsent(outerNestedPosition, it -> new ArrayList<>());
			blocks.add(nestedPosition);
		}
		return blocksPerShard.values();
	}

}
