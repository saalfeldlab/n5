package org.janelia.saalfeldlab.n5.readdata.segment;

import java.util.Comparator;

// TODO: If we use this to describe slices, it should be renamed probably!?
//       Ideas: Range, SliceLocation
public interface SegmentLocation {

	Comparator<SegmentLocation> COMPARATOR = Comparator
			.comparingLong(SegmentLocation::offset)
			.thenComparingLong(SegmentLocation::length);

	long offset();

	long length();

	default long end() {
		return offset() + length();
	}

	static SegmentLocation at(final long offset, final long length) {

		class DefaultSegmentLocation implements SegmentLocation {

			private final long offset;
			private final long length;

			public DefaultSegmentLocation(final long offset, final long length) {
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
				if (!(o instanceof DefaultSegmentLocation))
					return false;

				final DefaultSegmentLocation that = (DefaultSegmentLocation) o;
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

		return new DefaultSegmentLocation(offset, length);
	}
}
