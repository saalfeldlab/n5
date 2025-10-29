package org.janelia.saalfeldlab.n5.shard;


import java.util.ArrayList;

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
//     [+] Implement Comparable so that we can sort and aggregate for N5Reader.readBlocks(...).
//         For nested = {X,Y,Z} compare by Z, then Y, then X.
//         For X = {x,y,z} compare by z, then y, then x. (flattening order)
//
// TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import java.util.Arrays;
import java.util.List;

import org.janelia.saalfeldlab.n5.util.GridIterator;

public class Nesting {

	public static void main(String[] args) {
		final int[][] blockSizes = {{1,1,1}, {3,2,2}, {6,4,4}, {24,24,24}};
		final NestedGrid grid = new NestedGrid(blockSizes);
		final NestedPosition pos = new NestedPosition(grid, new long[] {38, 7, 129});
		System.out.println("pos = " + pos);
		System.out.println("key = " + Arrays.toString(pos.key()));
	}

	public static class NestedPosition implements Comparable<NestedPosition> {

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
			return grid.absolutePosition(position, level);
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

		@Override public int compareTo(NestedPosition o) {

			final int dimensionInequality = Integer.compare(numDimensions(), o.numDimensions());
			if (dimensionInequality != 0)
				return dimensionInequality;

			final int levelInequality = Integer.compare(level, o.level);
			if (levelInequality != 0)
				return levelInequality;

			final long[] otherAbsPos = o.absolute(level);
			final long[] absPos = absolute(level);

			for (int i = absPos.length - 1; i >= 0; --i) {
				final long diff = absPos[i] - otherAbsPos[i];
				if (diff != 0)
					return (int)diff;
			}

			return 0;
		}

		// TODO: equals() and hashCode()
		// TODO: should we have prefix()? suffix()? head()? tail()?
	}

	/**
	 * A nested grid of blocks used to coordinate the relationships of shards and the blocks / chunks they contain.
	 */
	public static class NestedGrid {

		private final int numLevels;

		private final int numDimensions;

		// relativeToBase[i][d] is block size at level i relative to level 0
		private final int[][] relativeToBase;

		// relativeToAdjacent[i][d] is block size at level i relative to level i-1
		private final int[][] relativeToAdjacent;

		private final int[][] blockSizes;

		/**
		 * {@code blockSizes[l][d]} is the block size at level {@code l} in dimension {@code d}.
		 * Level 0 contains the smallest blocks. blockSizes[l+1][d] must be a multiple of blockSizes[l][d].
		 *
		 * @param blockSizes
		 * 		block sizes for all levels and dimensions.
		 */
		public NestedGrid(int[][] blockSizes) {

			if (blockSizes == null)
				throw new IllegalArgumentException("blockSizes is null");

			if (blockSizes[0] == null)
				throw new IllegalArgumentException("blockSizes[0] is null");

			this.blockSizes = blockSizes;

			numLevels = blockSizes.length;
			numDimensions = blockSizes[0].length;
			relativeToBase = new int[numLevels][numDimensions];
			relativeToAdjacent = new int[numLevels][numDimensions];
			for (int l = 0; l < numLevels; ++l) {
				final int k = Math.max(0, l - 1);

				if (blockSizes[l] == null)
					throw new IllegalArgumentException("blockSizes[" + l + "] null");

				if (blockSizes[l].length != numDimensions)
					throw new IllegalArgumentException(
							String.format("Block size at level %d has a different length (%d vs %d)", l, numDimensions, blockSizes[l].length));

				for (int d = 0; d < numDimensions; ++d) {

					if (blockSizes[l][d] <= 0 ) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) is negative for dimension %d.",
										l, blockSizes[l][d], d));
					}

					if (blockSizes[l][d] < blockSizes[k][d]) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) is smaller than previous level (%d) "
										+ " for dimension %d.",
										l, blockSizes[l][d], blockSizes[k][d], d));
					}

					if (blockSizes[l][d] % blockSizes[k][d] != 0) {
						throw new IllegalArgumentException(
								String.format("Block sizes at level %d (%d) not a multiple of previous level (%d) "
										+ " for dimension %d.",
										l, blockSizes[l][d], blockSizes[k][d], d));
					}

					relativeToBase[l][d] = blockSizes[l][d] / blockSizes[0][d];
					relativeToAdjacent[l][d] = blockSizes[l][d] / blockSizes[k][d];
				}
			}
		}

		public int numLevels() {
			return numLevels;
		}

		public int numDimensions() {
			return numDimensions;
		}

		public int[] getBlockSize(int level) {
			return blockSizes[level];
		}

		public void absolutePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final long[] targetPos,
				final int targetLevel) {
			final int[] sk = relativeToBase[sourceLevel];
			final int[] si = relativeToBase[targetLevel];
			for (int d = 0; d < numDimensions; ++d) {
				targetPos[d] = sourcePos[d] * sk[d] / si[d];
			}
		}

		public void relativePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final long[] targetPos,
				final int targetLevel) {
			absolutePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			if (targetLevel < numLevels - 1) {
				final int[] rj = relativeToAdjacent[targetLevel + 1];
				for (int d = 0; d < numDimensions; ++d) {
					targetPos[d] %= rj[d];
				}
			}
		}

		/**
		 * The absolute position of * source position for the given source level at the target level.
		 * 
		 * @param sourcePos the source position j
		 * @param sourceLevel the source level
		 * @param targetLevel the target level
		 * @return absolute position at the target level
		 */
		public long[] absolutePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final int targetLevel) {
			final long[] targetPos = new long[numDimensions];
			absolutePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			return targetPos;
		}

		/**
		 * The absolute position of the level 0 source position at
		 * the target level.
		 * 
		 * @param sourcePos the source position j
		 * @param targetLevel the target level
		 * @return absolute position at the target level
		 */
		public long[] absolutePosition(
				final long[] sourcePos,
				final int targetLevel) {
			return absolutePosition(sourcePos, 0, targetLevel);
		}

		public long[] relativePosition(
				final long[] sourcePos,
				final int sourceLevel,
				final int targetLevel) {
			final long[] targetPos = new long[numDimensions];
			relativePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			return targetPos;
		}

		public long[] relativePosition(
				final long[] sourcePos,
				final int targetLevel) {
			return relativePosition(sourcePos, 0, targetLevel);
		}

		public int[] relativeBlockSize(final int level) {
			return relativeToAdjacent[level];
		}

		public int[] absoluteBlockSize(final int level) {
			return relativeToBase[level];
		}

		/**
		 * Given a block position at a particular level, returns a list of
		 * positions of all sub-blocks at a particular subLevel.
		 * <p>
		 * Can be used to get a list of chunk positions for a shard with a
		 * particular position.
		 *
		 * @param position
		 *            a position
		 * @param level
		 *            the nesting level of the given position
		 * @param subLevel
		 *            the nesting sub-level of positions to return
		 * @return the sub-block positions
		 */
		public List<long[]> positionInSubGrid(long[] position, int level, int subLevel) {

			final long[] subPosition = new long[numDimensions()];
			absolutePosition(position, level, subPosition, subLevel);

			final int[] numElementsInSubGrid = absoluteBlockSize(numLevels() - 1);
			final GridIterator git = new GridIterator(GridIterator.int2long(numElementsInSubGrid), subPosition);

			// TODO return NestedPositions instead?
			final ArrayList<long[]> positions = new ArrayList<>();
			while (git.hasNext())
				positions.add(git.next().clone());

			return positions;
		}

	}

}
