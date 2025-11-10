package org.janelia.saalfeldlab.n5.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bounds, in pixel coordinates, of a region in a dataset.
 * <p>
 * Provides methods to find which blocks and shards are contained in the
 * region, iterate sub-NestedPositions, etc.
 */
class Region {

	/**
	 * The dimensions of the full dataset.
	 * This is used to decide whether DataBlocks are on the border (and therefore possibly truncated).
	 * <p>
	 * TODO:
	 *   Currently, the constructor expands this to {@code min + size} (if that is larger than the provided datasetDimensions).
	 *   Do we want this, or should we rather throw an {@code }IllegalArgumentException} in that case?
	 */
	private final long[] datasetDimensions;

	/**
	 * The nested grid of the dataset
	 */
	private final Nesting.NestedGrid grid;

	/**
	 * min pixel position in the region
	 */
	private final long[] min;

	/**
	 * size of the region in pixels
	 */
	private final long[] size;

	/**
	 * {@code NestedPosition} of the block containing the min pixel position.
	 */
	private final Nesting.NestedPosition minPos;

	/**
	 * {@code NestedPosition} of the block containing the max pixel position.
	 */
	private final Nesting.NestedPosition maxPos;

	Region(final long[] min, final long[] size, final Nesting.NestedGrid grid, final long[] datasetDimensions) {
		this.min = min;
		this.size = size;
		this.grid = grid;

		final int n = min.length;
		this.datasetDimensions = new long[n];
		Arrays.setAll(this.datasetDimensions, d -> Math.max(min[d] + size[d], datasetDimensions[d]));

		final int[] blockSize = grid.getBlockSize(0);

		final long[] minBlock = new long[n];
		Arrays.setAll(minBlock, d -> min[d] / blockSize[d]);
		minPos = grid.nestedPosition(minBlock);

		final long[] maxBlock = new long[n];
		Arrays.setAll(maxBlock, d -> (min[d] + size[d] - 1) / blockSize[d]);
		maxPos = grid.nestedPosition(maxBlock);
	}

	/**
	 * Get the {@code NestedPosition} of the minimum DataBlock touched by the region.
	 */
	Nesting.NestedPosition minPos() {
		return minPos;
	}

	/**
	 * Get the {@code NestedPosition} of the maximum DataBlock touched by the region.
	 */
	Nesting.NestedPosition maxPos() {
		return maxPos;
	}

	/**
	 * Check whether the Shard or DataBlock corresponding to the given
	 * position is fully contained inside the region.
	 * (The {@link Nesting.NestedPosition#level() level} of {@code position} is used
	 * to determine whether it refers to a DataBlock or a (potentially
	 * nested) Shard.
	 *
	 * @param position
	 * 		the NestedPosition to check
	 *
	 * @return true, if the given position is fully contained in this region
	 */
	boolean fullyContains(final Nesting.NestedPosition position) {

		final long[] pmin = position.pixelPosition();
		for (int d = 0; d < pmin.length; d++) {
			if (pmin[d] < min[d]) {
				return false;
			}
		}

		final long[] pmax = maxPixelPos(position);
		for (int d = 0; d < pmax.length; d++) {
			final long m = Math.min(pmax[d], datasetDimensions[d] - 1);
			if (m > min[d] + size[d] - 1) {
				return false;
			}
		}

		return true;
	}

	// TODO: Revise. Inline? Should this be method of NestedPosition?
	private long[] maxPixelPos(final Nesting.NestedPosition position) {
		final long[] pos = position.pixelPosition();
		final int[] elementSize = grid.getBlockSize(position.level());
		Arrays.setAll(pos, d -> pos[d] + elementSize[d] - 1);
		return pos;
	}

	/**
	 * Returns {@code NestedPosition}s of all nested elements in the given
	 * {@code position} that are contained in this {@code Region}.
	 * <p>
	 * The returned {@code NestedPosition}s will all have level {@code
	 * position.level()-1}.
	 */
	// TODO: Revise to accept Consumer<NestedPosition> for handling each position
	List<Nesting.NestedPosition> containedNestedPositions(final Nesting.NestedPosition position) {
		final int level = position.level() - 1;
		final long[] gridMinOfRegion = minPos().absolute(level);
		final long[] gridMaxOfRegion = maxPos().absolute(level);

		final long[] gridMinOfPosition = position.absolute(level);
		final int[] gridSizeOfPosition = grid.relativeBlockSize(level + 1);

		final int n = grid.numDimensions();
		final long[] gridMin = new long[n];
		Arrays.setAll(gridMin, d -> Math.max(gridMinOfRegion[d], gridMinOfPosition[d]));
		final long[] gridMax = new long[n];
		Arrays.setAll(gridMax, d -> Math.min(gridMaxOfRegion[d], gridMinOfPosition[d] + gridSizeOfPosition[d] - 1));

		final List<long[]> gridPositions = gridPositions(gridMin, gridMax);
		final List<Nesting.NestedPosition> nestedPositions = new ArrayList<>();
		gridPositions.forEach(p -> nestedPositions.add(grid.nestedPosition(p, level)));
		return nestedPositions;
	}

	// TODO: Can this fully replace GridIterator?
	// TODO: Revise to accept Consumer<long[]> for handling each position
	static List<long[]> gridPositions(final long[] min, final long[] max) {
		final int n = min.length;
		final long[] pos = min.clone();
		int numElements = 1;
		for (int d = 0; d < n; ++d) {
			numElements *= (int) (max[d] - min[d] + 1);
		}
		long[][] positions = new long[numElements][n];
		Arrays.setAll(positions[0], j -> pos[j]);
		for (int i = 1; i < numElements; i++) {
			for (int d = 0; d < n; ++d) {
				if (++pos[d] <= max[d]) {
					Arrays.setAll(positions[i], j -> pos[j]);
					break;
				}
				pos[d] = min[d];
			}
		}
		return Arrays.asList(positions);
	}
}
