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


import java.util.Arrays;

public class Nesting {

	public static class NestedPosition implements Comparable<NestedPosition> {

		private final NestedGrid grid;
		private final long[] position;
		private final int level;

		protected NestedPosition(final NestedGrid grid, final long[] position, final int level) {
			this.grid = grid;
			this.position = position;
			this.level = level;
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
			return grid.relativePosition(position, this.level, level);
		}

		/**
		 * Get the relative grid position at this positions {@link #level()},
		 * that is, relative offset within containing the {@code (level+1)}
		 * element.
		 *
		 * @return relative grid position
		 */
		public long[] relative() {
			return relative(level());
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
			return grid.absolutePosition(position, this.level, level);
		}

		public long[] key() {
			return relative(grid.numLevels() - 1);
		}

		public long[] pixelPosition() {
			return grid.pixelPosition(position, level);
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

		@Override
		public int compareTo(NestedPosition o) {

			final int dimensionInequality = Integer.compare(numDimensions(), o.numDimensions());
			if (dimensionInequality != 0)
				return dimensionInequality;

			final int levelInequality = Integer.compare(level, o.level);
			if (levelInequality != 0)
				return levelInequality;

			for (int i = position.length - 1; i >= 0; --i) {
				final long diff = position[i] - o.position[i];
				if (diff != 0)
					return (int) diff;
			}

			return 0;
		}

		// TODO: equals() and hashCode()
		// TODO: should we have prefix()? suffix()? head()? tail()?
	}

	/**
	 * A nested grid of blocks used to coordinate the relationships of shards
	 * and the blocks / chunks they contain.
	 * <p>
	 * The nesting depth ({@link #numLevels()}) of the {@code NestedGrid} is 1
	 * for non-sharded datasets, 2 for simple sharded datasets (where shards
	 * contain datablocks), and &ge;3 for nested sharded datasets.
	 * <p>
	 * Positions with {@code level=0} refer to the DataBlock grid, positions
	 * with {@code level=1} refer to first-level Shard grid, and so on.
	 */
	public static class NestedGrid {

		/**
		 * Create a {@code NestedPosition} at the specified nesting {@code
		 * level} grid {@code position}.
		 * <p>
		 * Note that {@code position} is in units of grid elements at {@code
		 * level}. Positions with {@code level=0} refer to the DataBlock grid,
		 * positions with {@code level=1} refer to first-level Shard grid, and
		 * so on.
		 * <p>
		 * The returned {@code NestedPosition} will have
		 * {@link NestedPosition#level() level()==level}.
		 *
		 * @param position
		 * 		position at {@code level}
		 * @param level
		 * 		nesting level of {@code position}
		 *
		 * @return a NestedPosition representation of the specified grid position and nesting level
		 */
		public NestedPosition nestedPosition(final long[] position, final int level) {
			return new NestedPosition(this, position, level);
		}

		/**
		 * Create a {@code NestedPosition} at the specified block grid {@code
		 * position} (that is, at nesting level 0).
		 * <p>
		 * Note that {@code position} is in units of DataBlocks.
		 * <p>
		 * The returned {@code NestedPosition} will have
		 * {@link NestedPosition#level() level()==0}.
		 *
		 * @param position
		 * 		position at level 0 (block grid)
		 *
		 * @return a NestedPosition representation of the specified block grid position
		 */
		public NestedPosition nestedPosition(final long[] position) {
			return nestedPosition(position, 0);
		}






		private final int numLevels;

		private final int numDimensions;

		/**
		 * relativeToBase[i][d] is block size (in dimension d) at level i relative to level 0
		 */
		private final int[][] relativeToBase;

		/**
		 * relativeToAdjacent[i][d] is block size (in dimension d) at level i relative to level i-1
		 */
		private final int[][] relativeToAdjacent;

		/**
		 * blockSizes[l][d] is the block size in pixels at level l in dimension d
		 */
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

		/**
		 * Get the block size in pixels at the given {@code level}.
		 */
		public int[] getBlockSize(final int level) {
			return blockSizes[level];
		}

		/**
		 * Computes the pixel position for the given {@code sourcePos} grid
		 * position at {@code sourceLevel}.
		 *
		 * @param sourcePos
		 * 		a grid position at {@code sourceLevel}
		 * @param sourceLevel
		 * 		nesting level of {@code sourcePos}
		 * @param targetPos
		 * 		the pixel position will be stored here
		 */
		public void pixelPosition(
				final long[] sourcePos,
				final int sourceLevel,
				final long[] targetPos) {
			final int[] s = blockSizes[sourceLevel];
			for (int d = 0; d < numDimensions; ++d) {
				targetPos[d] = sourcePos[d] * s[d];
			}
		}

		/**
		 * Get the pixel position for the given {@code sourcePos} grid position
		 * at {@code sourceLevel}.
		 *
		 * @param sourcePos
		 * 		a grid position at {@code sourceLevel}
		 * @param sourceLevel
		 * 		nesting level of {@code sourcePos}
		 *
		 * @return the pixel position
		 */
		public long[] pixelPosition(
				final long[] sourcePos,
				final int sourceLevel) {
			final long[] targetPos = new long[numDimensions];
			pixelPosition(sourcePos, sourceLevel, targetPos);
			return targetPos;
		}

		/**
		 * Computes the absolute {@code targetPos} grid position at {@code
		 * targetLevel} for the given {@code sourcePos} grid position at {@code
		 * sourceLevel}.
		 * <p>
		 * For example, this can be used to compute the coordinates on the shard
		 * grid ({@code targetLevel==1}) of the shard containing a given
		 * datablock ({@code sourcePos} at {@code sourceLevel==0}).
		 *
		 * @param sourcePos
		 * 		a grid position at {@code sourceLevel}
		 * @param sourceLevel
		 * 		nesting level of {@code sourcePos}
		 * @param targetPos
		 * 		the grid position at {@code targetLevel} will be stored here
		 * @param targetLevel
		 * 		nesting level of {@code targetPos}
		 */
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

		/**
		 * Get the absolute grid position at {@code targetLevel} for the given
		 * {@code sourcePos} grid position at {@code sourceLevel}.
		 * <p>
		 * For example, this can be used to compute the coordinates on the shard
		 * grid ({@code targetLevel==1}) of the shard containing a given
		 * datablock ({@code sourcePos} at {@code sourceLevel==0}).
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
			if (sourceLevel == targetLevel) {
				return sourcePos;
			}
			final long[] targetPos = new long[numDimensions];
			absolutePosition(sourcePos, sourceLevel, targetPos, targetLevel);
			return targetPos;
		}

		/**
		 * Get the absolute grid position at {@code targetLevel} for the given
		 * {@code sourcePos} block grid position (level 0).
		 * <p>
		 * For example, this can be used to compute the coordinates on the shard
		 * grid ({@code targetLevel==1}) of the shard containing a given
		 * datablock ({@code sourcePos}.
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

		/**
		 * Computes the {@code targetPos} grid position at {@code targetLevel}
		 * for the given {@code sourcePos} grid position at {@code sourceLevel},
		 * relative to the containing element at {@code targetLevel+1}.
		 * (The containing element is a shard for {@code targetLevel+1 <
		 * numLevels} or the dataset for {@code targetLevel+1 == numLevels}.)
		 * <p>
		 * For example, this can be used to compute the grid coordinates {@code
		 * targetLevel==0} of a given datablock ({@code sourcePos} at {@code
		 * sourceLevel==0}) withing a shard (containing element at level {@code
		 * targetLevel+1==1}).
		 * </p>
		 *
		 * @param sourcePos
		 * 		a grid position at {@code sourceLevel}
		 * @param sourceLevel
		 * 		nesting level of {@code sourcePos}
		 * @param targetPos
		 * 		the grid position at {@code targetLevel} will be stored here
		 * @param targetLevel
		 * 		nesting level of {@code targetPos}
		 */
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

		/**
		 * Get size of a block at the given {@code level} relative to {@code
		 * level-1} (that is, in units of {@code level-1} blocks).
		 * <p>
		 * For example {@code relativeBlockSize(1)} returns the number of
		 * datablocks in a (non-nested) shard.
		 */
		public int[] relativeBlockSize(final int level) {
			return relativeToAdjacent[level];
		}
	}
}
