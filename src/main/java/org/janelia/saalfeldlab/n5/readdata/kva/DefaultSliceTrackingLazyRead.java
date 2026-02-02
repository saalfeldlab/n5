package org.janelia.saalfeldlab.n5.readdata.kva;

import java.util.Collection;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;

public class DefaultSliceTrackingLazyRead extends SliceTrackingLazyRead {

	public DefaultSliceTrackingLazyRead(final LazyRead delegate) {
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

	private boolean isCovered(final Range slice) {

		return Slices.findContainingSlice(slices, slice) != null;
	}
}
