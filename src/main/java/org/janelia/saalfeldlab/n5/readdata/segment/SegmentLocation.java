package org.janelia.saalfeldlab.n5.readdata.segment;

import java.util.Comparator;

public interface SegmentLocation {

	Comparator<SegmentLocation> COMPARATOR = Comparator
			.comparingLong(SegmentLocation::offset)
			.thenComparingLong(SegmentLocation::length);

	long offset();

	long length();

	static SegmentLocation at(final long offset, final long length) {
		return new DefaultSegmentLocation(offset, length);
	}
}
