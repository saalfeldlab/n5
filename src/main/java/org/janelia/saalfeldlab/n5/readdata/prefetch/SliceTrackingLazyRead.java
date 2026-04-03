package org.janelia.saalfeldlab.n5.readdata.prefetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link LazyRead} that wraps a delegate {@code LazyRead} and keeps track of
 * all slices that have been {@link #materialize materialized}.
 * <p>
 * When materializing a new slice, we first check whether it is completely
 * covered by a materialized slice that we already track. If so, then we just
 * return a slice on the existing materialized slice. If not, we materialize the
 * slice from the delegate track it.
 */
public class SliceTrackingLazyRead implements LazyRead {

	protected static class Slice implements Range {

		// Offset and length in the delegate
		private final long offset;
		private final long length;

		// Data of this slice
		private final ReadData data;

		Slice(final long offset, final long length, final ReadData data) {
			this.offset = offset;
			this.length = length;
			this.data = data;
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
		public String toString() {
			return "{" + offset + ", " + length + '}';
		}
	}

	protected final List<Slice> slices = new ArrayList<>();

	/**
	 * The {@code LazyRead} providing our data.
	 */
	private final LazyRead delegate;

	public SliceTrackingLazyRead(final LazyRead delegate) {
		this.delegate = delegate;
	}

	@Override
	public void close() throws IOException {
		delegate.close();
	}

	@Override
	public ReadData materialize(final long offset, final long length) throws N5IOException {
		final Slice containing = Slices.findContainingSlice(slices, offset, length);
		if (containing != null) {
			return containing.data.slice(offset - containing.offset, length);
		} else {
			final ReadData data = delegate.materialize(offset, length);
			Slices.addSlice(slices, new Slice(offset, length, data));
			return data;
		}
	}

	@Override
	public long size() throws N5IOException {
		return delegate.size();
	}

	protected boolean isCovered(final Range slice) {

		return Slices.findContainingSlice(slices, slice) != null;
	}
}
