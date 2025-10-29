package org.janelia.saalfeldlab.n5.readdata;

import java.util.Comparator;

/**
 * A range specified as a {@link #offset}, {@link #length} pair.
 */
public interface Range {

	/**
	 * Order {@code Range}s by {@link #offset}.
	 * Ranges with the same offset are ordered by {@link #length}.
	 */
	Comparator<Range> COMPARATOR = Comparator
			.comparingLong(Range::offset)
			.thenComparingLong(Range::length);

	/**
	 * @return start index (inclusive)
	 */
	long offset();

	/**
	 * @return number of elements
	 */
	long length();

	/**
	 * @return end index (exclusive)
	 */
	default long end() {
		return offset() + length();
	}

	static boolean equals(final Range r0, final Range r1) {
		if (r0 == null && r1==null) {
			return true;
		} else if (r0 == null || r1 == null) {
			return false;
		} else {
			return r0.offset() == r1.offset() && r0.length() == r1.length();
		}
	}

	static Range at(final long offset, final long length) {

		class DefaultRange implements Range {

			private final long offset;
			private final long length;

			public DefaultRange(final long offset, final long length) {
				this.offset = offset;
				this.length = length;
			}

			@Override
			public long offset() {
				return offset;
			}

			@Override
			public long length() {
				return length;
			}

			@Override
			public final boolean equals(final Object o) {
				if (!(o instanceof DefaultRange))
					return false;

				final DefaultRange that = (DefaultRange) o;
				return offset == that.offset && length == that.length;
			}

			@Override
			public int hashCode() {
				int result = Long.hashCode(offset);
				result = 31 * result + Long.hashCode(length);
				return result;
			}

			@Override
			public String toString() {
				return "Range{" +
						"offset=" + offset +
						", length=" + length +
						'}';
			}
		}

		return new DefaultRange(offset, length);
	}
}
