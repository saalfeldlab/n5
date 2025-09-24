package org.janelia.saalfeldlab.n5.shardstuff;


// TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// [ ] NestedGrid
//     [ ] validation in constructor
//     [ ] test for that validation
//     [ ] javadoc
//
// [ ] NestedPosition interface
//     [+] LazyNestedPosition class
//         [+] fields: NestedGrid, long[] position, int level
//         [+] construct with source level 0
//     [+] minimal abs/rel access methods
//     [+] toString()
//     [-] extract NestedPosition interface
//         ==> postpone until necessary
//     [ ] equals / hashcode
//     [ ] should we have prefix()? suffix()? head()? tail()?
//     [ ] Implement Comparable so that we can sort and aggregate for N5Reader.readBlocks(...).
//         For nested = {X,Y,Z} compare by Z, then Y, then X.
//         For X = {x,y,z} compare by z, then y, then x. (flattening order)
//
// TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import java.util.Arrays;

public class Nesting {

	public static void main(String[] args) {
		final int[][] blockSizes = {{1,1,1}, {3,2,2}, {6,4,4}, {24,24,24}};
		final NestedGrid grid = new NestedGrid(blockSizes);
		final NestedPosition pos = new NestedPosition(grid, new long[] {38, 7, 129});
		System.out.println("pos = " + pos);
		System.out.println("key = " + Arrays.toString(pos.key()));
	}




	public static class NestedPosition {

		private final NestedGrid grid;
		private final long[] position;
		private final int level;

		public NestedPosition(final NestedGrid grid, final long[] position, final int level) {
			this.grid = grid;
			this.position = position;
			this.level = level;
		}

		public NestedPosition(final NestedGrid grid, final long[] position) {
			this(grid, position, 0);
		}

		/**
		 * Get the nesting level of this position.
		 * <p>
		 * Positions with {@code level=0} refer to DataBlocks, positions with
		 * {@code level=1} refer to first-level shards (containing DataBlocks),
		 * and so on.
		 *
		 * @return nesting level
		 */
		public int level() {
			return level;
		}

		public int numDimensions() {
			return grid.numDimensions();
		}

		/**
		 * Get the relative grid position at {@code level}, that is, relative
		 * offset within containing the {@code (level+1)} element.
		 *
		 * @param level
		 * 		requested nesting level
		 *
		 * @return relative grid position
		 */
		public long[] relative(final int level) {
			return grid.relativePosition(position, level);
		}

		/**
		 * Get the absolute grid position at {@code level}.
		 *
		 * @param level
		 * 		requested nesting level
		 *
		 * @return absolute grid position
		 */
		public long[] absolute(final int level) {
			return grid.relativePosition(position, level);
		}

		public long[] key() {
			return relative(grid.numLevels() - 1);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('{');
			for ( int l = level; l < grid.numLevels(); ++l ) {
				if ( l > level ) {
					sb.append(" / ");
				}
				sb.append(Arrays.toString(relative(l)));
			}
			sb.append(" (level ").append(level).append(")}");
			return sb.toString();
		}

		// TODO: equals() and hashCode()
		// TODO: should we have prefix()? suffix()? head()? tail()?
	}


	/**
	 * TODO
	 */
	public static class NestedGrid {

		// num levels
		private final int m;

		// num dimensions
		private final int n;

		// s[i][d] is block size at level i relative to level 0
		private final int[][] s;

		// r[i][d] is block size at level i relative to level i-1
		private final int[][] r;

		/**
		 * {@code blockSizes[l][d]} is the block size at level {@code l} in dimension {@code d}.
		 * Level 0 is the highest resolution (smallest block sizes).
		 *
		 * @param blockSizes
		 * 		block sizes for all levels and dimensions.
		 */
		public NestedGrid(int[][] blockSizes) {

			if (blockSizes == null)
				throw new IllegalArgumentException("blockSizes is null");

			if (blockSizes[0] == null)
				throw new IllegalArgumentException("blockSizes[0] is null");

			m = blockSizes.length;
			n = blockSizes[0].length;
			s = new int[m][n];
			r = new int[m][n];
			for (int l = 0; l < m; ++l) {
				final int k = Math.max(0, l - 1);

				if (blockSizes[l] == null)
					throw new IllegalArgumentException("blockSizes[" + l + "] null");

				if (blockSizes[l].length != n)
					throw new IllegalArgumentException(
							String.format("Block size at level %d has a different length (%d vs %d)", l, n, blockSizes[l].length));

				for (int d = 0; d < n; ++d) {

					if (blockSizes[l][d] <= 0 ) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) is negative for dimension %d.",
										l, blockSizes[l][d], d));
					}

					if (blockSizes[l][d] > blockSizes[k][d]) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) is larger than previous level (%d) "
										+ " for dimension %d.",
										l, blockSizes[l][d], blockSizes[k][d], d));
					}

					if (blockSizes[k][d] % blockSizes[l][d] != 0) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) not a multiple of previous level (%d) "
										+ " for dimension %d.",
										l, blockSizes[l][d], blockSizes[k][d], d));
					}

					s[l][d] = blockSizes[l][d] / blockSizes[0][d];
					r[l][d] = blockSizes[l][d] / blockSizes[k][d];
				}
			}
		}

		public int numLevels() {
			return m;
		}

		public int numDimensions() {
			return n;
		}

		public void absolutePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final long[] targetPos,
				final int targetLevel) {
			final int[] sk = s[sourceLevel];
			final int[] si = s[targetLevel];
			for (int d = 0; d < n; ++d) {
				targetPos[d] = sourcePos[d] * sk[d] / si[d];
			}
		}

		public void relativePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final long[] targetPos,
				final int targetLevel) {
			absolutePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			if (targetLevel < m - 1) {
				final int[] rj = r[targetLevel + 1];
				for (int d = 0; d < n; ++d) {
					targetPos[d] %= rj[d];
				}
			}
		}

		public long[] absolutePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final int targetLevel) {
			final long[] targetPos = new long[n];
			absolutePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			return targetPos;
		}

		public long[] absolutePosition(
				final long[] sourcePos,
				final int targetLevel) {
			return absolutePosition(sourcePos, 0, targetLevel);
		}

		public long[] relativePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final int targetLevel) {
			final long[] targetPos = new long[n];
			relativePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			return targetPos;
		}

		public long[] relativePosition(
				final long[] sourcePos,
				final int targetLevel) {
			return relativePosition(sourcePos, 0, targetLevel);
		}

		public int[] relativeBlockSize(final int level) {
			return r[level];
		}
	}


}
