package org.janelia.saalfeldlab.n5.util;

import java.util.Arrays;

/*
 * A wrapper around a primitive long array that is lexicographically {@link Comparable}
 * and for which we can test equality.
 */
public interface Position extends Comparable<Position> {

	long[] get();

	long get(int i);

	default int numDimensions() {
		return get().length;
	}

	@Override
	default int compareTo(Position other) {

		// use Arrays.compare when we update to Java 9+
		final int N = numDimensions() > other.numDimensions() ? numDimensions() : other.numDimensions();
		for (int i = 0; i < N; i++) {
			final long diff = get(i) - other.get(i);
			if (diff != 0)
				return (int) diff;
		}
		return 0;
	}

	static boolean equals(final Position a, final Object b) {

		if (a == null && b == null)
			return true;

		if (b == null)
			return false;

		if (!(b instanceof Position))
			return false;

		final Position other = (Position) b;
		if (other.numDimensions() != a.numDimensions())
			return false;

		for (int i = 0; i < a.numDimensions(); i++)
			if (other.get(i) != a.get(i))
				return false;

		return true;
	}

	static String toString(Position p) {
		return "Position: " + Arrays.toString(p.get());
	}

	static Position wrap(final long[] p) {
		return new FinalPosition(p);
	}

	static Position wrap(final int[] p) {
		return new FinalPosition(GridIterator.int2long(p));
	}

}
