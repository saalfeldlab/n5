package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

class DefaultSegmentedReadData implements SegmentedReadData {

	/**
	 * The {@code ReadData} providing our data. This is either {@code segmentSource} in
	 * which case {@code offset==0}, or a slice into {@code segmentSource} in
	 * which case {@code offset} is the offset of the slice.
	 */
	private final ReadData delegate;

	/**
	 * The {@link Segment#source() source} {@code ReadData} of all contained
	 * segments.
	 */
	private final ReadData segmentSource;

	/**
	 * The offset of {@code delegate} withr espect to {@code segmentSource}.
	 */
	private final long offset;

	/**
	 * Contained segments, ordered by location.
	 */
	private final List<SegmentImpl> segments;

	private static class SegmentImpl implements Segment, SegmentLocation {

		private final SegmentedReadData source;
		private final long offset;
		private final long length;

		public SegmentImpl(final SegmentedReadData source, final SegmentLocation location) {
			this(source, location.offset(), location.length());
		}

		public SegmentImpl(final SegmentedReadData source, final long offset, final long length) {
			this.source = source;
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
		public SegmentedReadData source() {
			return source;
		}
	}

	private static class EnclosingSegmentImpl extends SegmentImpl {

		public EnclosingSegmentImpl(final SegmentedReadData source) {
			super(source, 0, -1);
		}

		@Override
		public long length() {
			return source().length();
		}
	}

	// assumes segments are ordered by location
	private DefaultSegmentedReadData(final ReadData delegate, final ReadData segmentSource, final long offset, final List<SegmentImpl> segments) {
		this.delegate = delegate;
		this.segmentSource = segmentSource;
		this.offset = offset;
		this.segments = segments;
	}

	// assumes segments are ordered by location
	private DefaultSegmentedReadData(final ReadData delegate, final List<SegmentImpl> segments) {
		this.delegate = delegate;
		this.segmentSource = this;
		this.offset = 0;
		this.segments = segments;
	}

	static SegmentsAndData wrap(final ReadData readData, final List<SegmentLocation> locations) {
		final List<SegmentImpl> sortedSegments = new ArrayList<>(locations.size());
		final DefaultSegmentedReadData data = new DefaultSegmentedReadData(readData, sortedSegments);
		for (SegmentLocation l : locations) {
			sortedSegments.add(new SegmentImpl(data, l));
		}
		final List<Segment> segments = new ArrayList<>(sortedSegments);
		sortedSegments.sort(SegmentLocation.COMPARATOR);

		return new SegmentsAndData() {

			@Override
			public List<Segment> segments() {
				return segments;
			}

			@Override
			public SegmentedReadData data() {
				return data;
			}
		};
	}

	private DefaultSegmentedReadData(final ReadData delegate) {
		this.delegate = delegate;
		this.segmentSource = this;
		this.offset = 0;
		this.segments = Collections.singletonList(new EnclosingSegmentImpl(this));
	}

	static SegmentedReadData wrap(final ReadData readData) {
		return new DefaultSegmentedReadData(readData);
	}

	@Override
	public SegmentLocation location(final Segment segment) {
		if (segmentSource.equals(segment.source()) && segment instanceof SegmentLocation) {
			final SegmentLocation l = (SegmentLocation) segment;
			return offset == 0 ? l : SegmentLocation.at(l.offset() - offset, l.length());
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public List<? extends Segment> segments() {
		return segments;
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
	public SegmentedReadData slice(final Segment segment) throws IllegalArgumentException, N5IOException {
		if (segmentSource.equals(segment.source())) {
			if (segment instanceof EnclosingSegmentImpl) {
				return this;
			} else if (segment instanceof SegmentImpl) {
				final SegmentImpl s = (SegmentImpl) segment;
				return new DefaultSegmentedReadData(
						delegate.slice(s.offset(), s.length()),
						segmentSource,
						this.offset + s.offset(),
						Collections.singletonList(s));
			}
		}
		throw new IllegalArgumentException();
	}

	@Override
	public SegmentedReadData slice(final long offset, final long length) throws N5IOException {

		final ReadData delegateSlice = delegate.slice(offset, length);
		final long sourceOffset = this.offset + offset;
		final long sliceLength = delegateSlice.length();


		// fromIndex: find first segment with offset >= sourceOffset
		int fromIndex = Collections.binarySearch(segments, SegmentLocation.at(sourceOffset, -1), SegmentLocation.COMPARATOR);
		if (fromIndex < 0) {
			fromIndex = -fromIndex - 1;
		}

		// toIndex: find first segment with offset >= sourceOffset + length
		int toIndex;
		if (sliceLength < 0) {
			toIndex = segments.size();
		} else {
			toIndex = Collections.binarySearch(segments, SegmentLocation.at(sourceOffset + sliceLength, -1), SegmentLocation.COMPARATOR);
			if (toIndex < 0) {
				toIndex = -toIndex - 1;
			}
		}

		// contained: find segments in [fromIndex, toIndex) with s.offset() + s.length() <= sourceOffset + length
		final List<SegmentImpl> candidates = segments.subList(fromIndex, toIndex);
		final List<SegmentImpl> sliceSegments;
		if (sliceLength < 0) {
			sliceSegments = candidates;
		} else {
			final ArrayList<SegmentImpl> contained = new ArrayList<>(candidates.size());
			candidates.forEach(s -> {
				if (s.offset() + s.length() <= sourceOffset + sliceLength) {
					contained.add(s);
				}
			});
			contained.trimToSize();
			sliceSegments = contained;
		}

		return new DefaultSegmentedReadData(
				delegateSlice,
				segmentSource,
				sourceOffset,
				sliceSegments);
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
	public SegmentedReadData materialize() throws N5IOException {
		delegate.materialize();
		return this;
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
		delegate.writeTo(outputStream);
	}

	@Override
	public void prefetch(final Collection<? extends SegmentLocation> ranges) throws N5IOException {
		delegate.prefetch(ranges);
	}

	/**
	 * Returns a new ReadData that uses the given {@code OutputStreamOperator} to
	 * encode this SegmentedReadData.
	 * <p>
	 * Note that segments are lost by encoding.
	 *
	 * @param encoder
	 * 		OutputStreamOperator to use for encoding
	 *
	 * @return encoded ReadData
	 */
	@Override
	public ReadData encode(final OutputStreamOperator encoder) {
		return delegate.encode(encoder);
	}
}
