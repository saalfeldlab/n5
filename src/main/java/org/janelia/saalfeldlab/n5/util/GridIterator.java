package org.janelia.saalfeldlab.n5.util;

import java.util.Iterator;

/**
 * Essentially imglib2's IntervalIterator, but N5 does not depend on imglib2.
 */
public class GridIterator implements Iterator<long[]> {

	final protected long[] dimensions;

	final protected long[] steps;

	final protected long[] position;

	final protected long[] min;

	final protected int lastIndex;

	protected int index = -1;

	public GridIterator(final long[] dimensions, final long[] min) {

		final int n = dimensions.length;
		this.dimensions = new long[n];
		this.position = new long[n];
		this.min = min;
		steps = new long[n];

		final int m = n - 1;
		long k = steps[0] = 1;
		for (int d = 0; d < m;) {
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

	public int[] nextAsInt() {
		return long2int(next());
	}

	public int getIndex() {
		return index;
	}

	final static public void indexToPosition(long index, final long[] dimensions, final long[] min, final long[] position) {
		final int maxDim = dimensions.length - 1;
		for (int dim = maxDim; dim >= 0; dim--) {
			position[dim] = index % dimensions[dim] + min[dim];
			index /= dimensions[dim];
		}
	}

	final static public int[] long2int(final long[] a) {
		final int[] i = new int[a.length];

		for (int d = 0; d < a.length; ++d)
			i[d] = (int) a[d];

		return i;
	}

	final static public long[] int2long(final int[] i) {
		final long[] l = new long[i.length];

		for (int d = 0; d < l.length; ++d)
			l[d] = i[d];

		return l;
	}

}
