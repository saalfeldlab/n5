package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

class Concatenate implements SegmentedReadData {

	private final List<SegmentedReadData> content;
	private final ReadData delegate;
	private final List<Segment> segments;
	private final List<SegmentLocation> locations;
	private final Map<Segment, SegmentLocation> segmentToLocation;
	private boolean locationsBuilt;
	private long length;

	Concatenate(final List<SegmentedReadData> content) {
		this.content = content;
		delegate = ReadData.from(os -> content.forEach(d -> d.writeTo(os)));
		segments = new ArrayList<>();
		locations = new ArrayList<>();
		segmentToLocation = new HashMap<>();
		locationsBuilt = false;
		length = -1;
	}

	// constructor for slices
	private Concatenate(final ReadData delegate, final List<Segment> segments,
			final List<SegmentLocation> locations) {
		content = null;
		this.delegate = delegate;
		this.segments = segments;
		this.locations = locations;
		segmentToLocation = new HashMap<>();
		for (int i = 0; i < segments.size(); i++) {
			segmentToLocation.put(segments.get(i), locations.get(i));
		}
		locationsBuilt = true;
	}

	/**
	 * Verify that all {@code content} elements have known length.
	 * Builds {@code segments} and {@code locations} if they have not been built yet.
	 *
	 * @throws IllegalStateException if any of the concatenated ReadData don't know their length yet
	 */
	private void ensureKnownSize() throws IllegalStateException {
		if (!locationsBuilt) {
			long offset = 0;
			for (int i = 0; i < content.size(); i++) {
				final SegmentedReadData data = content.get(i);
				if (data.length() < 0) {
					throw new IllegalStateException("Some of concatenated ReadData don't know their length yet.");
				}
				segments.addAll(data.segments());
				for (Segment segment : data.segments()) {
					final SegmentLocation l = data.location(segment);
					locations.add(SegmentLocation.at(l.offset() + offset, l.length()));
				}
				offset += data.length();
			}
			length = offset;

			for (int i = 0; i < segments.size(); i++) {
				segmentToLocation.put(segments.get(i), locations.get(i));
			}

			locationsBuilt = true;
		}
	}

	@Override
	public SegmentLocation location(final Segment segment) throws IllegalArgumentException {
		ensureKnownSize();
		final SegmentLocation location = segmentToLocation.get(segment);
		if (location == null) {
			throw new IllegalArgumentException();
		}
		return location;
	}

	@Override
	public List<? extends Segment> segments() {
		return segments;
	}

	@Override
	public long length() {
		if (length < 0) {
			length = 0;
			for (final ReadData data : content) {
				final long l = data.length();
				if (l < 0) {
					length = -1;
					break;
				}
				length += l;
			}
		}
		return length;
	}

	@Override
	public long requireLength() throws N5IOException {
		if (length < 0) {
			length = 0;
			for (final ReadData data : content) {
				length += data.requireLength();
			}
		}
		return length;
	}

	@Override
	public SegmentedReadData slice(final Segment segment) throws IllegalArgumentException, N5IOException {
		ensureKnownSize();
		final SegmentLocation l = location(segment);
		return slice(l.offset(), l.length());
	}

	@Override
	public SegmentedReadData slice(final long offset, final long length) throws N5IOException {
		ensureKnownSize();
		final ReadData delegateSlice = delegate.slice(offset, length);
		final long sliceLength = delegateSlice.length();

		// fromIndex: find first segment with offset >= sourceOffset
		int fromIndex = Collections.binarySearch(locations, SegmentLocation.at(offset, -1), SegmentLocation.COMPARATOR);
		if (fromIndex < 0) {
			fromIndex = -fromIndex - 1;
		}

		// toIndex: find first segment with offset >= sourceOffset + length
		int toIndex = Collections.binarySearch(locations, SegmentLocation.at(offset + sliceLength, -1), SegmentLocation.COMPARATOR);
		if (toIndex < 0) {
			toIndex = -toIndex - 1;
		}

		// contained: find segments in [fromIndex, toIndex) with s.offset() + s.length() <= sourceOffset + length
		final List<Segment> containedSegments = new ArrayList<>();
		final List<SegmentLocation> containedSegmentLocations = new ArrayList<>();
		for (int i = fromIndex; i < toIndex; ++i) {
			final SegmentLocation l = locations.get(i);
			if (l.offset() + l.length() <= offset + sliceLength) {
				containedSegments.add(segments.get(i));
				containedSegmentLocations.add(SegmentLocation.at(l.offset() - offset, l.length()));
			}
		}

		return new Concatenate(delegateSlice, containedSegments, containedSegmentLocations);
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
	public ReadData encode(final OutputStreamOperator encoder) {
		return delegate.encode(encoder);
	}
}
