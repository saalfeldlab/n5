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
		return new DefaultSegmentLocation(offset, length);
	}
}
