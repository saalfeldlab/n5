package org.janelia.saalfeldlab.n5.readdata.kva;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class KvaPlayground {


	public interface VolatileReadData extends ReadData, AutoCloseable {}

	static class DelegatingReadData implements ReadData {

		protected final ReadData delegate;

		protected DelegatingReadData(final ReadData delegate) {
			this.delegate = delegate;
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
		public ReadData limit(final long length) throws N5IOException {
			return delegate.limit(length);
		}

		@Override
		public ReadData slice(final long offset, final long length) throws N5IOException {
			return delegate.slice(offset, length);
		}

		@Override
		public ReadData slice(final Range range) throws N5IOException {
			return delegate.slice(range);
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
			return this;
		}

		@Override
		public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
			delegate.writeTo(outputStream);
		}

		@Override
		public void prefetch(final Collection<? extends Range> ranges) throws N5IOException {
			delegate.prefetch(ranges);
		}

		@Override
		public ReadData encode(final OutputStreamOperator encoder) {
			return delegate.encode(encoder);
		}
	}

	/**
	 * An {@code AutoCloseable} {@code ReadData}.
	 * <p>
	 * The close() method simply calls a Runnable provided on construction.
	 * All ReadData methods are forwarded to a delegate. In particular,
	 * slices will not be Closable.
	 */
	static class ClosableReadData extends DelegatingReadData implements VolatileReadData {

		private final Runnable close;

		ClosableReadData(final ReadData delegate, final Runnable close) {
			super(delegate);
			this.close = close;
		}

		@Override
		public void close() {
			close.run();
		}
	}


	// Playground for prototyping the ReadData that we get from a KeyValueAccess.
	//
	// This includes the following features:
	// (*) LazyRead
	//     This interface will be extended to be Closable.
	//     The FileSystem implementation creates a LockedChannel on construction and releases it on close()
	//     Actual read requests create their own channels.
	//     New read requests should fail when the LazyRead is already closed.
	// (*) KvaReadData
	//     Implemented via LazyRead.
	// (*) ClosableReadData
	//     A ReadData that implements Closable.
	//     The close() method simply calls a Runnable provided on construction.
	//     All ReadData methods are forwarded to a delegate. In particular,
	//     slices will not be Closable.
	// (*) LazySlicingReadData
	//     A ReadData whose slice methods produce also LazySlicingReadData.
	//     Only when materializing methods are called, a slice is obtained from
	//     the delegate and the operation carried out on that.
	// (*) SliceTrackingReadData
	//     When a slice is taken, the returned slice is already materialized.
	//     Keeps an internal cache of (materialized) slices and first tries to
	//     satisfy slice requests from this cache. If a slice request cannot be obtained from the cache,
	//     the slice is taken from the delegate and materialized.
	//     This is supposed to be wrapped with LazySlicingReadData.
	//     The combination of LazySlicingReadData and SliceTrackingReadData
	//     allows to take slices (and slices of slices) that are only
	//     materialized when necessary, but if materialized are cached correctly.
	//
	// Clever handling of ReadData::inputStream is postponed until later --
	// LazySlicingReadData will materialize when an inputStream is requested.
	//
	// The ReadData with all features that KeyValueAccess can provide is:
	//   LazyRead -> KvaReadData -> SliceTrackingReadData -> LazySlicingReadData -> ClosableReadData
	// FileKeyValueAccess should probably only do this:
	//   LazyRead -> KvaReadData -> ClosableReadData
	// (But for testing we can put everything there as well.)
	//
	// TODO:
	//   [+] Let LazyRead extends Closable
	//   [+] Implement KvaReadData (mostly copy from KeyValueAccessReadData) --> LazyReadData
	//   [+] Implement ClosableReadData


	// Revised plan:
	// KvaReadData/KeyValueAccessReadData was renamed to LazyReadData.
	// It behaves exactly as the LazySlicingReadData above, so we don't need that.
	// SliceTrackingReadData should become SliceTrackingLazyRead.
	// The chain then looks like this:
	//   LazyRead -> SliceTrackingLazyRead -> LazyReadData -> ClosableReadData
	// and for FileKeyValueAccess:
	//   LazyRead -> LazyReadData -> ClosableReadData
	// (But for testing we can put everything there as well.)

	// TODO:
	//   [+] Move LazyRead out of KeyValueAccess
	//   [+] Revise SliceTrackingReadData to be a LazyRead instead
	//   [ ] Implement the following behaviour for FileLazyRead:
	//         Create a LockedChannel on construction and releases it on close()
	//         Actual read requests create their own channels.
	//         New read requests should fail when the LazyRead is already closed.
	//   [ ] DefaultSegmentedReadData should extend DelegatingReadData (if we add that as a general utility)
	//   [ ] propagate prefetch() through delegates and eventually to the LazyRead. (Also, add prefetch() to LazyRead interface).
}
