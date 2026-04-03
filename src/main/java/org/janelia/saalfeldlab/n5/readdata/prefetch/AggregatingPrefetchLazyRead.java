package org.janelia.saalfeldlab.n5.readdata.prefetch;

import java.util.ArrayList;
import java.util.Collection;

import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.Range;

/**
 * A {@link SliceTrackingLazyRead} that implements {@link #prefetch} to
 * aggregate overlapping / adjacent ranges and then materialize each aggregated
 * range.
 */
public class AggregatingPrefetchLazyRead extends SliceTrackingLazyRead {

	public AggregatingPrefetchLazyRead(final LazyRead delegate) {
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

		final List<Range> filteredRanges = new ArrayList<>(ranges);
		filteredRanges.removeIf(this::isCovered);
		final Collection<? extends Range> aggregatedRanges = Range.aggregate(filteredRanges);
		for (final Range slice : aggregatedRanges) {
			materialize(slice.offset(), slice.length());
		}
	}

}
