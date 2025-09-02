package org.janelia.saalfeldlab.n5.readdata.segment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.SegmentLocation;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.SegmentLocationImpl;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentStuff.SegmentedReadData;

class SegmentedReadDataImpl extends SegmentStuff.ReadDataWrapper implements SegmentedReadData {

	// segments, ordered by location
	private final List<SegmentImpl> segments;

	private final ReadData segmentSource;

	private final long offset;

	private static class SegmentImpl implements Segment, SegmentLocation {

		private final ReadData source;

		private final long offset;

		private final long length;

		public SegmentImpl(final ReadData source, final SegmentLocation location) {
			this(source, location.offset(), location.length());
		}

		public SegmentImpl(final ReadData source, final long offset, final long length) {
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
		public ReadData source() {
			return source;
		}
	}

	// assumes segments are ordered by location
	private SegmentedReadDataImpl(final ReadData delegate, final ReadData segmentSource, final long offset, final List<SegmentImpl> segments) {
		super(delegate);
		this.segmentSource = segmentSource;
		this.offset = offset;
		this.segments = segments;
	}

	// assumes segments are ordered by location
	private SegmentedReadDataImpl(final ReadData delegate, final List<SegmentImpl> segments) {
		this(delegate, delegate, 0, segments);
	}

	// TODO: does not assume ordered locations. to not lose track, this method
	//       should return Pair<SegmentedReadData, Segment[]> where segments are in
	//       the same order as locations.
	// TODO: use List<SegmentLocation> instead of SegmentLocation[]
	public static SegmentedReadData wrap(final ReadData readData, final SegmentLocation[] locations) {
		final List<SegmentImpl> segments = new ArrayList<>(locations.length);
		for (SegmentLocation l : locations) {
			segments.add(new SegmentImpl(readData, l));
		}
		segments.sort(SegmentLocation.COMPARATOR);
		return new SegmentedReadDataImpl(readData, segments);
	}

	@Override
	public SegmentLocation location(final Segment segment) {
		if (segmentSource.equals(segment.source()) && segment instanceof SegmentLocation) {
			final SegmentLocation l = (SegmentLocation) segment;
			return offset == 0 ? l : new SegmentLocationImpl(l.offset() - offset, l.length());
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public List<? extends Segment> segments() {
		return segments;
	}

	@Override
	public SegmentedReadData slice(final Segment segment) throws IllegalArgumentException, N5Exception.N5IOException {
		if (segmentSource.equals(segment.source()) && segment instanceof SegmentImpl) {
			final SegmentImpl s = (SegmentImpl) segment;
			return new SegmentedReadDataImpl(
					delegate.slice(s.offset(), s.length()),
					segmentSource,
					this.offset + s.offset(),
					Collections.singletonList(s));
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public SegmentedReadData slice(final long offset, final long length) throws N5Exception.N5IOException {

		final long sourceOffset = this.offset + offset;

		// fromIndex: find first segment with offset >= sourceOffset
		int fromIndex = Collections.binarySearch(segments, new SegmentLocationImpl(sourceOffset, 0), SegmentLocation.COMPARATOR);
		if (fromIndex < 0) {
			fromIndex = -fromIndex - 1;
		}

		// toIndex: find first segment with offset >= sourceOffset + length
		int toIndex = Collections.binarySearch(segments, new SegmentLocationImpl(sourceOffset + length, 0), SegmentLocation.COMPARATOR);
		if (toIndex < 0) {
			toIndex = -toIndex - 1;
		}

		// contained: find segments in [fromIndex, toIndex) with s.offset() + s.length() <= sourceOffset + length
		final List<SegmentImpl> candidates = segments.subList(fromIndex, toIndex);
		final ArrayList<SegmentImpl> contained = new ArrayList<>(candidates.size());
		candidates.forEach(s -> {
			if (s.offset() + s.length() <= sourceOffset + length) {
				contained.add(s);
			}
		});
		contained.trimToSize();

		return new SegmentedReadDataImpl(
				delegate.slice(offset, length),
				segmentSource,
				this.offset + offset,
				contained);
	}
}
