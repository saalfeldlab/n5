package org.janelia.saalfeldlab.n5.readdata.kva;

import java.util.Collection;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;

public class AggregatingSliceTrackingLazyRead extends SliceTrackingLazyRead {

	public AggregatingSliceTrackingLazyRead(final LazyRead delegate) {
		super(delegate);
	}

	/**
	 * Indicates that the given slices will be subsequently read.
	 * <p>
	 * This implementation groups overlapping / adjacent {@link Range}s into single read requests.
	 *
	 * @param ranges
	 * 		slice ranges to prefetch
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	@Override
	public void prefetch(final Collection<? extends Range> ranges) throws N5IOException {

		final Collection<? extends Range> aggregatedRanges = Range.aggregate(ranges);
		for (final Range slice : aggregatedRanges) {
			materialize(slice.offset(), slice.length());
		}
	}

}
