/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Implementation of a {@link SegmentedReadData} representing the concatenation
 * of several {@code SegmentedReadData}s.
 * <p>
 * {@code ConcatenatedReadData} contains the segments of all concatenated {@code
 * SegmentedReadData}s with appropriately offset locations.
 * <p>
 * In particular, it is also possible to concatenate {@code SegmentedReadData}s
 * with (yet) unknown length. (This is useful for postponing compression of
 * DataBlocks until they are actually written.) In that case, segment locations
 * are only available, after all lengths become known. This happens when this
 * {@code ConcatenatedReadData} (or all its constituents) is
 * {@link #materialize() materialized} or {@link #writeTo(OutputStream) written}.
 */
class ConcatenatedReadData implements SegmentedReadData {

	private final List<SegmentedReadData> content;
	private final ReadData delegate;
	private final List<Segment> segments;
	private final List<Range> locations;
	private final Map<Segment, Range> segmentToLocation;
	private boolean locationsBuilt;
	private long length;

	ConcatenatedReadData(final List<SegmentedReadData> content) {
		this.content = content;
		delegate = ReadData.from(os -> content.forEach(d -> d.writeTo(os)));
		segments = new ArrayList<>();
		locations = new ArrayList<>();
		segmentToLocation = new HashMap<>();
		locationsBuilt = false;
		length = -1;
	}

	// constructor for slices
	private ConcatenatedReadData(final ReadData delegate, final List<Segment> segments,
			final List<Range> locations) {
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
					final Range l = data.location(segment);
					locations.add(Range.at(l.offset() + offset, l.length()));
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
	public Range location(final Segment segment) throws IllegalArgumentException {
		ensureKnownSize();
		final Range location = segmentToLocation.get(segment);
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
		final Range l = location(segment);
		return slice(l.offset(), l.length());
	}

	@Override
	public SegmentedReadData slice(final long offset, final long length) throws N5IOException {
		ensureKnownSize();
		final ReadData delegateSlice = delegate.slice(offset, length);
		final long sliceLength = delegateSlice.length();

		// fromIndex: find first segment with offset >= sourceOffset
		int fromIndex = Collections.binarySearch(locations, Range.at(offset, -1), Range.COMPARATOR);
		if (fromIndex < 0) {
			fromIndex = -fromIndex - 1;
		}

		// toIndex: find first segment with offset >= sourceOffset + length
		int toIndex = Collections.binarySearch(locations, Range.at(offset + sliceLength, -1), Range.COMPARATOR);
		if (toIndex < 0) {
			toIndex = -toIndex - 1;
		}

		// contained: find segments in [fromIndex, toIndex) with s.offset() + s.length() <= sourceOffset + length
		final List<Segment> containedSegments = new ArrayList<>();
		final List<Range> containedSegmentLocations = new ArrayList<>();
		for (int i = fromIndex; i < toIndex; ++i) {
			final Range l = locations.get(i);
			if (l.offset() + l.length() <= offset + sliceLength) {
				containedSegments.add(segments.get(i));
				containedSegmentLocations.add(Range.at(l.offset() - offset, l.length()));
			}
		}

		return new ConcatenatedReadData(delegateSlice, containedSegments, containedSegmentLocations);
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
