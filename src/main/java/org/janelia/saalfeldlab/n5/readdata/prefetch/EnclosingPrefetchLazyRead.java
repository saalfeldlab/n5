package org.janelia.saalfeldlab.n5.readdata.prefetch;

import java.util.Collection;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.Range;

/**
 * A {@link SliceTrackingLazyRead} that implements {@link #prefetch} to
 * materialize the bounding range of all requested ranges.
 */
public class EnclosingPrefetchLazyRead extends SliceTrackingLazyRead {

	public EnclosingPrefetchLazyRead(final LazyRead delegate) {
		super(delegate);
	}

	/**
	 * Indicates that the given slices will be subsequently read.
	 * {@code LazyRead} implementations (optionally) may take steps to prepare
	 * for these subsequent slices.
	 * <p>
	 * Minimal implementation: Find offset and length covering all ranges that
	 * are not yet fully covered by existing slices. Then materialize the slice
	 * covering that range.
	 *
	 * @param ranges
	 * 		slice ranges to prefetch
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	@Override
	public void prefetch(final Collection<? extends Range> ranges) throws N5IOException {

		long fromIndex = Long.MAX_VALUE;
		long toIndex = Long.MIN_VALUE;
		for (final Range slice : ranges) {
			if (!isCovered(slice)) {
				fromIndex = Math.min(fromIndex, slice.offset());
				toIndex = Math.max(toIndex, slice.end());
			}
		}

		if (fromIndex < toIndex) {
			materialize(fromIndex, toIndex - fromIndex);
		}
	}
}
