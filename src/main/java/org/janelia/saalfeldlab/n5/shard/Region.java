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
import java.util.List;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;

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
	 */
	private final long[] datasetDimensions;

	/**
	 * The nested grid of the dataset
	 */
	private final NestedGrid grid;

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

	Region(final long[] min, final long[] size, final NestedGrid grid) {
		this.min = min;
		this.size = size;
		this.grid = grid;
		this.datasetDimensions = grid.getDatasetSize();

		final int n = min.length;
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

		final long[] pmax = position.maxPixelPosition();
		for (int d = 0; d < pmax.length; d++) {
			final long m = Math.min(pmax[d], datasetDimensions[d] - 1);
			if (m > min[d] + size[d] - 1) {
				return false;
			}
		}

		return true;
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
