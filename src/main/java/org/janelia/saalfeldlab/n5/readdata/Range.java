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
package org.janelia.saalfeldlab.n5.readdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
	 *		a collection of Ranges 
	 * @return
	 */
	static Collection<? extends Range> aggregate(final Collection<? extends Range> ranges) {

		if (ranges.size() == 0)
			return ranges;

		ArrayList<Range> sortedRanges = new ArrayList<>(ranges);
		Collections.sort(sortedRanges, Range.COMPARATOR);

		ArrayList<Range> result = new ArrayList<>();
		boolean wereMerges = false;
		Range lo = null;
		for (Range hi : sortedRanges) {

			if (lo == null)
				lo = hi;
			else if (lo.end() >= hi.offset()) {
				// merge
				final Range mergedLo = Range.at(lo.offset(), Math.max(lo.end(), hi.end()) - lo.offset());
				lo = mergedLo;
				wereMerges = true;
			} else {
				result.add(lo);
				lo = hi;
			}
		}
		result.add(lo);

		if (!wereMerges)
			return ranges;

		return result;
	}	

}
