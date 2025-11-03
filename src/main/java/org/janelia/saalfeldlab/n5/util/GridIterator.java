package org.janelia.saalfeldlab.n5.util;

import java.util.Iterator;

/**
 * Essentially imglib2's IntervalIterator, but N5 does not depend on imglib2.
 */
public class GridIterator implements Iterator<long[]> {

	final protected long[] dimensions;

	final protected long[] steps;

	final protected long[] position;

	final protected int[] intPosition;

	final protected long[] min;

	final protected int lastIndex;

	protected int index = -1;

	public GridIterator(final long[] dimensions, final long[] min) {

		final int n = dimensions.length;
		this.dimensions = new long[n];
		this.position = new long[n];
		this.intPosition = new int[n];
		this.min = min;
		steps = new long[n];

		final int m = n - 1;
		long k = steps[0] = 1;
		for (int d = 0; d < m; ) {
			final long dimd = dimensions[d];
			this.dimensions[d] = dimd;
			k *= dimd;
			steps[++d] = k;
		}
		final long dimm = dimensions[m];
		this.dimensions[m] = dimm;
		lastIndex = (int)(k * dimm - 1);
	}

	public GridIterator(final long[] dimensions) {

		this(dimensions, new long[dimensions.length]);
	}

	public GridIterator(final int[] dimensions) {

		this(int2long(dimensions));
	}

	public void fwd() {

		++index;
	}

	public void reset() {

		index = -1;
	}

	@Override
	public boolean hasNext() {

		return index < lastIndex;
	}

	@Override
	public long[] next() {

		fwd();
		indexToPosition(index, dimensions, min, position);
		return position;
	}

	public int[] nextInt() {

		next();
		long2int(position, intPosition);
		return intPosition;
	}

	public int getIndex() {

		return index;
	}

	public static void indexToPosition(long index, final long[] dimensions, final long[] offset,
			final long[] position) {

		for (int dim = 0; dim < dimensions.length; dim++) {
			position[dim] = (index % dimensions[dim]) + offset[dim];
			index /= dimensions[dim];
		}
	}

	public static void indexToPosition(long index, final int[] dimensions, final long[] offset,
			final long[] position) {

		for (int dim = 0; dim < dimensions.length; dim++) {
			position[dim] = (index % dimensions[dim]) + offset[dim];
			index /= dimensions[dim];
		}
	}

	final static public long positionToIndex(final long[] dimensions, final long[] position) {
		long idx = 0;
		int cumulativeSize = 1;
		for (int i = 0; i < position.length; i++) {
			idx += position[i] * cumulativeSize;
			cumulativeSize *= dimensions[i];
		}
		return idx;
	}

	final static public long positionToIndex(final long[] dimensions, final int[] position) {
		long idx = 0;
		int cumulativeSize = 1;
		for (int i = 0; i < position.length; i++) {
			idx += position[i] * cumulativeSize;
			cumulativeSize *= dimensions[i];
		}
		return idx;
	}

	final static public long positionToIndex(final int[] dimensions, final long[] position) {
		long idx = 0;
		int cumulativeSize = 1;
		for (int i = 0; i < position.length; i++) {
			idx += position[i] * cumulativeSize;
			cumulativeSize *= dimensions[i];
		}
		return idx;
	}

	final static public long positionToIndex(final int[] dimensions, final int[] position) {
		long idx = 0;
		int cumulativeSize = 1;
		for (int i = 0; i < position.length; i++) {
			idx += position[i] * cumulativeSize;
			cumulativeSize *= dimensions[i];
		}
		return idx;
	}

	public static int[] long2int(final long[] src, final int[] tgt) {

		for (int d = 0; d < tgt.length; ++d)
			tgt[d] = (int)src[d];

		return tgt;
	}

	public static int[] long2int(final long[] a) {

		final int[] i = new int[a.length];

		for (int d = 0; d < a.length; ++d)
			i[d] = (int)a[d];

		return i;
	}

	public static long[] int2long(final int[] i) {

		final long[] l = new long[i.length];

		for (int d = 0; d < l.length; ++d)
			l[d] = i[d];

		return l;
	}

}
