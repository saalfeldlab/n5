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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.junit.Assert;
import org.junit.Test;

public class NestedGridTest {

	private static long absPosition1D(final NestedGrid grid, final int sourcePos, final int targetLevel) {
		return grid.absolutePosition(new long[] {sourcePos}, targetLevel)[0];
	}

	@Test
	public void testValidateInput() {
		int[][] blockSizes = {{3}, {7}, {11}};
		assertThrows(IllegalArgumentException.class, () -> new NestedGrid(blockSizes));
	}

	@Test
	public void testAbsolutePosition() {
		int[][] blockSizes = {{1}, {3}, {6}, {24}};
		NestedGrid grid = new NestedGrid(blockSizes);

		Assert.assertEquals(38, absPosition1D(grid, 38, 0));

		Assert.assertEquals(12, absPosition1D(grid, 36, 1));
		Assert.assertEquals(12, absPosition1D(grid, 37, 1));
		Assert.assertEquals(12, absPosition1D(grid, 38, 1));

		Assert.assertEquals(6, absPosition1D(grid, 38, 2));
		Assert.assertEquals(1, absPosition1D(grid, 38, 3));
	}

	@Test
	public void testAbsolutePositionChunkSize() {
		int[][] blockSizes = {{10}, {30}, {60}, {240}};
		NestedGrid grid = new NestedGrid(blockSizes);

		Assert.assertEquals(38, absPosition1D(grid, 38, 0));
		Assert.assertEquals(12, absPosition1D(grid, 38, 1));
		Assert.assertEquals(6, absPosition1D(grid, 38, 2));
		Assert.assertEquals(1, absPosition1D(grid, 38, 3));
	}

	private static long relPosition1D(final NestedGrid grid, final int sourcePos, final int targetLevel) {
		return grid.relativePosition(new long[] {sourcePos}, targetLevel)[0];
	}

	@Test
	public void testRelativePosition() {
		int[][] blockSizes = {{1}, {3}, {6}, {24}};
		NestedGrid grid = new NestedGrid(blockSizes);

		Assert.assertEquals(2, relPosition1D(grid, 38, 0));
		Assert.assertEquals(0, relPosition1D(grid, 38, 1));
		Assert.assertEquals(2, relPosition1D(grid, 38, 2));
		Assert.assertEquals(1, relPosition1D(grid, 38, 3));

	}

	@Test
	public void testRelativePositionChunkSize() {
		int[][] blockSizes = {{10}, {30}, {60}, {240}};
		NestedGrid grid = new NestedGrid(blockSizes);

		Assert.assertEquals(2, relPosition1D(grid, 38, 0));
		Assert.assertEquals(0, relPosition1D(grid, 38, 1));
		Assert.assertEquals(2, relPosition1D(grid, 38, 2));
		Assert.assertEquals(1, relPosition1D(grid, 38, 3));
	}

	@Test
	public void testNd() {

		int[][] blockSizes = {{5, 7}, {5*3, 7*2}};
		NestedGrid grid = new NestedGrid(blockSizes);
		System.out.println(grid);
		assertArrayEquals(new long[]{1, 2}, grid.absolutePosition(new long[]{1, 2}, 0));
		assertArrayEquals(new long[]{99, 99}, grid.absolutePosition(new long[]{99, 99}, 0));

		assertArrayEquals(new long[]{0, 0}, grid.absolutePosition(new long[]{0, 1}, 1));
		assertArrayEquals(new long[]{0, 1}, grid.absolutePosition(new long[]{0, 2}, 1));
		assertArrayEquals(new long[]{1, 1}, grid.absolutePosition(new long[]{3, 2}, 1));
	}
}
