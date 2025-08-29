package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.junit.Assert;
import org.junit.Test;

public class NestedGridTest {

	private static long absPosition1D(final NestedGrid grid, final int sourcePos, final int targetLevel) {
		return grid.absolutePosition(new long[] {sourcePos}, targetLevel)[0];
	}

	@Test
	public void testAbsolutePosition() {
		int[][] blockSizes = {{1}, {3}, {6}, {24}};
		NestedGrid grid = new NestedGrid(blockSizes);

		Assert.assertEquals(38, absPosition1D(grid, 38, 0));
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
}
