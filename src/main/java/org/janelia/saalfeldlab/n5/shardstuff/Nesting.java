package org.janelia.saalfeldlab.n5.shardstuff;


// TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//
// [ ] NestedGrid javadoc
// [ ] NestedPosition interface
//
// TODO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


import java.util.Arrays;

public class Nesting {


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

		// NB: as usual, level 0 is full resolution.
		// TODO: BlockCodec stuff does this differently at the moment, but that should be changed ...

		/**
		 * {@code blockSizes[l][d]} is the block size at level {@code l} in dimension {@code d}.
		 * Level 0 is the highest resolution (smallest block sizes).
		 *
		 * @param blockSizes
		 * 		block sizes for all levels and dimensions.
		 */
		public NestedGrid(int[][] blockSizes) {

			// TODO: validate
			//       [ ] not null
			//       [ ] nesteds not null
			//       [ ] nesteds same length
			//       [ ] sizes match up to integer multiples

			m = blockSizes.length;
			n = blockSizes[0].length;
			s = new int[m][n];
			r = new int[m][n];
			for (int l = 0; l < m; ++l) {
				final int k = Math.max(0, l - 1);
				for (int d = 0; d < n; ++d) {
					s[l][d] = blockSizes[l][d] / blockSizes[0][d];
					r[l][d] = blockSizes[l][d] / blockSizes[k][d];
				}
			}
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
	}

}
