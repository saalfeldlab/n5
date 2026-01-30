package org.janelia.saalfeldlab.n5.readdata.kva;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class SliceTrackingLazyRead implements LazyRead {

	private static class Slice implements Range {

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

	private final List<Slice> slices = new ArrayList<>();

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
//	@Override // TODO: add this to the LazyRead interface? We don't know which slices have already been loaded anywhere else...
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
