package org.janelia.saalfeldlab.n5.readdata.segment;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.janelia.saalfeldlab.n5.readdata.Range;

class Slices {

	private Slices() {
		// utility class. should not be instantiated.
	}

	/**
	 * In an ordered list of {@code slices}, find a slice that completely contains the given range.
	 * <p>
	 * Pre-conditions:
	 * <ol>
	 * <li>Slices are ordered by offset.</li>
	 * <li>If two slices overlap, no slice is fully contained within the other.
	 *     (Therefore, if {@code a.offset < b.offset} then {@code a.end < b.end}.)</li>
	 * </ol>
	 *
	 * @param slices
	 * 		ordered list of slices
	 * @param offset
	 * 		start of the range to cover
	 * @param length
	 * 		length of the range to cover
	 *
	 * @return a slice that completely contains the requested range, or {@code null} if no such slice exists
	 */
	static <T extends Range> T findContainingSlice(final List<T> slices, final long offset, final long length) {

		// Find the slice with the largest slice.offset <= offset.
		final int i = Collections.binarySearch(slices, Range.at(offset, 0), Comparator.comparingLong(Range::offset));

		// Largest index of a slice with slice.offset <= offset.
		final int index = i < 0 ? -i - 2 : i;
		if (index < 0) {
			// We find no overlapping slice, because
			// slices[0].offset is already too large.
			return null;
		}

		final T slice = slices.get(index);
		if (slice.end() < offset + length) {
			return null;
		}

		return slice;
	}

	/**
	 * In an ordered list of {@code slices}, find a slice that completely contains the given range.
	 * <p>
	 * Pre-conditions:
	 * <ol>
	 * <li>Slices are ordered by offset.</li>
	 * <li>If two slices overlap, no slice is fully contained within the other.
	 *     (Therefore, if {@code a.offset < b.offset} then {@code a.end < b.end}.)</li>
	 * </ol>
	 *
	 * @param slices
	 * 		ordered list of slices
	 * @param range
	 * 		range to cover
	 *
	 * @return a slice that completely contains the requested range, or {@code null} if no such slice exists
	 */
	static <T extends Range> T findContainingSlice(final List<T> slices, final Range range) {
		return findContainingSlice(slices, range.offset(), range.length());
	}

	/**
	 * Add a new {@code slice} to the {@code slice} list.
	 * <p>
	 * Note, that the new {@code slice} is expected to not be fully contained in
	 * an existing slice!
	 * <p>
	 * Pre/post-conditions:
	 * <ol>
	 * <li>Slices are ordered by offset.</li>
	 * <li>If two slices overlap, no slice is fully contained within the other.
	 *     (Therefore, if {@code a.offset < b.offset} then {@code a.end < b.end}.)</li>
	 * </ol>
	 * <p>
	 * The new {@code slice} will be inserted into the list at the correct position
	 * (such that {@code slices} remains ordered by slice offset), and all existing
	 * slices that are fully contained in the new {@code slice} will be removed.
	 *
	 * @param slices
	 * 		ordered list of slices
	 * @param slice
	 * 		slice to be inserted
	 */
	static <T extends Range> void addSlice(final List<T> slices, final T slice) {

		final int i = Collections.binarySearch(slices, slice, Comparator.comparingLong(Range::offset));
		final int from = i < 0 ? -i - 1 : i;

		int to = from;
		while (to < slices.size() && slices.get(to).end() <= slice.end()) {
			++to;
		}

		if (from == to) {
			// empty range: just insert
			slices.add(from, slice);
		} else {
			// overwrite the first element in range, remove the rest
			slices.set(from, slice);
			slices.subList(from + 1, to).clear();
		}
	}
}
