package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class SegmentStuff {

	public interface SegmentedReadData extends ReadData {

		/**
		 * Returns the {@code SegmentLocation} of {@code segment} in this {@code ReadData}.
		 * <p>
		 * Note that this {@code ReadData} is not necessarily the source of the segment.
		 * <p>
		 * The returned {@code SegmentLocation} may be {@code {offset=0, index=-1}}, which
		 * means that the segment comprises this whole {@code ReadData} (and the length of
		 * this {@code ReadData} is not yet known.
		 *
		 * @param segment
		 * 		the segment id
		 *
		 * @return location of the segment, or null
		 * @throws IllegalArgumentException if the segment is not contained in this ReadData
		 */
		SegmentLocation location(Segment segment) throws IllegalArgumentException;

		/**
		 * @return all segments contained in this {@code ReadData}.
		 */
		// Order is the same as the SegmentLocations given at construction
		List<? extends Segment> segments();

		// TODO: return wrapper of readData with one segment comprising the whole ReadData
		//       length of segment could return readData.length()
		static SegmentedReadData wrap(ReadData readData) {
			throw new UnsupportedOperationException("TODO");
		}

		/**
		 * Wrap {@code readData} and create segments at the given locations.
		 */
		// TODO: does not assume ordered locations. to not lose track, this method
		//       should return Pair<SegmentedReadData, Segment[]> where segments are in
		//       the same order as locations.
		// TODO: use List<SegmentLocation> instead of SegmentLocation[]
		static SegmentedReadData wrap(ReadData readData, SegmentLocation... locations) {
			return SegmentedReadDataImpl.wrap(readData, locations);
		}

		/**
		 * Return a {@code SegmentedReadData} wrapping a slice containing exactly the given segment.
		 *
		 * @param segment
		 * @return
		 * @throws IllegalArgumentException if the segment is not contained in this ReadData
		 * @throws N5IOException
		 */
		SegmentedReadData slice(Segment segment) throws IllegalArgumentException, N5IOException;

		// TODO: has all segments fully contained in requested slice.
		@Override
		SegmentedReadData slice(final long offset, final long length) throws N5IOException;

	}

	/**
	 * A particular segment in a source {@code ReadData}.
	 */
	public interface Segment {

		ReadData source();
	}

	public interface SegmentLocation {

		Comparator<SegmentLocation> COMPARATOR = Comparator
				.comparingLong(SegmentLocation::offset)
				.thenComparingLong(SegmentLocation::length);

		long offset();

		long length();

		static SegmentLocation at(final long offset, final long length) {
			return new SegmentLocationImpl(offset, length);
		}
	}



	// TODO rename
	static class SegmentedReadData_Single extends ReadDataWrapper implements SegmentedReadData {

		private class SegmentImpl implements Segment, SegmentLocation {

			@Override
			public ReadData source() {
				return delegate;
			}

			@Override
			public long offset() {
				return 0;
			}

			@Override
			public long length() {
				return delegate.length();
			}
		}

		private final SegmentImpl segment = new SegmentImpl();

		SegmentedReadData_Single(final ReadData delegate) {
			super(delegate);
		}

		@Override
		public SegmentLocation location(final Segment s) {
			return (segment.equals(s)) ? segment : null;
		}

		@Override
		public List<Segment> segments() {
			return Collections.singletonList(segment);
		}

		@Override
		public SegmentedReadData slice(final Segment s) throws IllegalArgumentException, N5IOException {
			if (segment.equals(s)) {
				return this;
			} else {
				throw new IllegalArgumentException("Provided segment is not part of this ");
			}
		}

		@Override
		public SegmentedReadData slice(final long offset, final long length) throws N5IOException {
			final ReadData delegateSlice = delegate.slice(offset, length);
			if (offset == segment.offset() && length == segment.length()) {
				return this;
			} else {
				throw new UnsupportedOperationException("TODO");
			}
		}
	}


	static class SegmentImpl implements Segment {

		private final ReadData source;

		// TODO: add Segment id for debugging ...

		public SegmentImpl(final ReadData source) {
			this.source = source;
		}

		@Override
		public ReadData source() {
			return source;
		}
	}

	static class SegmentLocationImpl implements SegmentLocation {

		private final long offset;
		private final long length;

		public SegmentLocationImpl(final long offset, final long length) {
			this.offset = offset;
			this.length = length;
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
			return "SegmentLocation{" +
					"offset=" + offset +
					", length=" + length +
					'}';
		}
	}

	static class ReadDataWrapper implements ReadData {
		final ReadData delegate;

		ReadDataWrapper(final ReadData delegate) {
			this.delegate = delegate;
		}

		@Override
		public long length() throws N5IOException {
			return delegate.length();
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
			return delegate.materialize();
		}

		@Override
		public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
			delegate.writeTo(outputStream);
		}

		@Override
		public ReadData encode(final OutputStreamOperator encoder) {
			return delegate.encode(encoder);
		}
	}


}
