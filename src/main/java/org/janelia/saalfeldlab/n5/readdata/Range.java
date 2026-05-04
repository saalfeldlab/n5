package org.janelia.saalfeldlab.n5.readdata;

import java.util.ArrayList;
import java.util.Collection;
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
		if (r0 == null && r1 == null) {
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

	/**
	 * Returns a potentially new collection of {@link Range}s such that 
	 * adjacent or overlapping Ranges are combined. 
	 * <p>
	 * If the input ranges are non-adjacent the input instance is returned, but if aggregation 
	 * occurs, the result will be a new, sorted List.
	 *
	 * @param ranges
	 * 		a collection of Ranges
	 *
	 * @return collection with adjacent or overlapping ranges merged
	 */
	static Collection<? extends Range> aggregate(final Collection<? extends Range> ranges) {

		if (ranges.size() == 0)
			return ranges;

		final ArrayList<Range> sortedRanges = new ArrayList<>(ranges);
		sortedRanges.sort(Range.COMPARATOR);

		final ArrayList<Range> result = new ArrayList<>();
		Range lo = null;
		for (Range hi : sortedRanges) {

			if (lo == null)
				lo = hi;
			else if (lo.end() >= hi.offset()) {
				// merge
				lo = Range.at(lo.offset(), Math.max(lo.end(), hi.end()) - lo.offset());
			} else {
				result.add(lo);
				lo = hi;
			}
		}
		result.add(lo);

		final boolean wereMerges = ranges.size() != result.size();
		return wereMerges ? result : ranges;
	}

}
