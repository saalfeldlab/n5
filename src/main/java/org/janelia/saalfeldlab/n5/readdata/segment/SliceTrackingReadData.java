package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

class SliceTrackingReadData implements ReadData {

	private static class Slice implements SegmentLocation {

		private final long offset;
		private final long length;
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
	 * The {@code ReadData} providing our data.
	 */
	private final ReadData delegate;

	private SliceTrackingReadData(final ReadData delegate) {
		this.delegate = delegate;
	}

	static ReadData wrap(final ReadData readData) {
		return new SliceTrackingReadData(readData);
	}

	@Override
	public long length() {
		return delegate.length();
	}

	@Override
	public long requireLength() throws N5IOException {
		return delegate.requireLength();
	}

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {
		final Slice containing = Slices.findContainingSlice(slices, offset, length);
		if (containing != null) {
			return containing.data.slice(offset - containing.offset, length);
		} else {
			final ReadData data = delegate.slice(offset, length);
			Slices.addSlice(slices, new Slice(offset, length, data));
			return data;
		}
	}

	@Override
	public InputStream inputStream() throws N5IOException, IllegalStateException {
		return delegate.inputStream();
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {
		return delegate.allBytes();
	}

	@Override
	public ByteBuffer toByteBuffer() throws N5IOException, IllegalStateException {
		return delegate.toByteBuffer();
	}

	@Override
	public ReadData materialize() throws N5IOException {
		delegate.materialize();
		if (slices.size() != 1 || slices.get(0).data != delegate) {
			slices.clear();
			slices.add(new Slice(0, length(), delegate));
		}
		return this;
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
		delegate.writeTo(outputStream);
	}

	@Override
	public ReadData encode(final OutputStreamOperator encoder) {
		return delegate.encode(encoder);
	}


	// --- -- - prefetching - -- ---

	/**
	 * Indicates that the given slices will be subsequently read.
	 * {@code ReadData} implementations (optionally) may take steps to prepare
	 * for these subsequent slices.
	 */
	// TODO: where to put this? Could be in ReadData interface with empty default implementation?
	public void prefetch(final Collection<? extends SegmentLocation> slices) {

	}
}
