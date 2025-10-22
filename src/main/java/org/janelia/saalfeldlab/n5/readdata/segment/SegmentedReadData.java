package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@code ReadData} which keeps track of contained {@link Segment}s.
 * <p>
 * A {@code Segment} refers to <em>the data</em> in a particular {@link Range}.
 * (That range can be obtained using {@link #location(Segment)}). Segments are
 * pulled along when {@link #slice(long, long)} slicing} or {@link #concatenate
 * concatenating} {@code SegmentedReadData} (and will have appropriately offset
 * {@link #location locations} in the slice/concatenation).
 */
public interface SegmentedReadData extends ReadData {

	interface SegmentsAndData {
		List<Segment> segments();
		SegmentedReadData data();
	}

	/**
	 * Wrap {@code readData} and create one segment comprising the entire {@code
	 * readData}. The segment can be retrieved as the first (and only) element
	 * of {@link SegmentedReadData#segments()}.
	 */
	static SegmentedReadData wrap(ReadData readData) {
		return new DefaultSegmentedReadData(SliceTrackingReadData.wrap(readData));
	}

	/**
	 * Wrap {@code readData} and create segments at the given locations. The
	 * order of segments in the returned {@link SegmentsAndData#segments()} list
	 * matches the order of the given {@code locations} (while the {@link
	 * #segments} in the {@link SegmentsAndData#data()} are ordered by offset).
	 */
	static SegmentsAndData wrap(ReadData readData, Range... locations) {
		return wrap(readData, Arrays.asList(locations));
	}

	/**
	 * Wrap {@code readData} and create segments at the given locations. The
	 * order of segments in the returned {@link SegmentsAndData#segments()} list
	 * matches the order of the given {@code locations} (while the {@link
	 * #segments} in the {@link SegmentsAndData#data()} are ordered by offset).
	 */
	static SegmentsAndData wrap(ReadData readData, List<Range> locations) {
		return DefaultSegmentedReadData.wrap(SliceTrackingReadData.wrap(readData), locations);
	}

	/**
	 * Return a {@link SegmentedReadData} representing the concatenation of the
	 * given {@code readDatas}. The concatenation contains the segments of all
	 * concatenated {@code readData}s with appropriately offset locations.
	 * <p>
	 * In particular, it is also possible to concatenate {@code SegmentedReadData}s
	 * with (yet) unknown length. (This is useful for postponing compression of
	 * DataBlocks until they are actually written.) In that case, segment locations
	 * are only available after all lengths become known. This happens when
	 * concatenation (or all its constituents) is {@link #materialize()
	 * materialized} or {@link #writeTo(OutputStream) written}.
	 */
	static SegmentedReadData concatenate(List<SegmentedReadData> readDatas) {
		return new ConcatenatedReadData(readDatas);
	}



	/**
	 * Returns the location of {@code segment} in this {@code ReadData}.
	 * <p>
	 * Note that this {@code ReadData} is not necessarily the source of the segment.
	 * <p>
	 * The returned {@code Range} may be {@code {offset=0, length=-1}}, which
	 * means that the segment comprises this whole {@code ReadData} (and the length of
	 * this {@code ReadData} is not yet known).
	 *
	 * @param segment
	 * 		the segment id
	 *
	 * @return location of the segment, or null
	 *
	 * @throws IllegalArgumentException
	 * 		if the segment is not contained in this ReadData
	 */
	Range location(Segment segment) throws IllegalArgumentException;

	/**
	 * Return all segments (fully) contained in this {@code ReadData}, ordered by location
	 * (that is, sorted by {@link Range#COMPARATOR}).
	 *
	 * @return all segments contained in this {@code ReadData}.
	 */
	List<? extends Segment> segments();

	@Override
	default SegmentedReadData limit(final long length) throws N5Exception.N5IOException {
		return slice(0, length);
	}

	/**
	 * Return a {@code SegmentedReadData} wrapping a slice containing exactly
	 * the given segment.
	 * <p>
	 * The {@link #location} of the given {@code segment} in this ReadData
	 * specifies the range to slice.
	 *
	 * @param segment segment to slice
	 * @return a slice
	 * @throws IllegalArgumentException
	 * 		if the segment is not contained in this ReadData
	 * @throws N5Exception.N5IOException
	 */
	SegmentedReadData slice(Segment segment) throws IllegalArgumentException, N5Exception.N5IOException;

	/**
	 * Returns a new {@link SegmentedReadData} representing a slice, or subset
	 * of this ReadData.
	 * <p>
	 * The {@link #segments} of the returned SegmentedReadData are all segments
	 * fully contained in the requested range.
	 *
	 * @param offset the offset relative to this ReadData
	 * @param length length of the returned ReadData
	 * @return a slice
	 * @throws N5Exception.N5IOException an exception
	 */
	@Override
	SegmentedReadData slice(long offset, long length) throws N5Exception.N5IOException;

	/**
	 * Returns a new {@link SegmentedReadData} representing a slice, or subset
	 * of this ReadData.
	 * <p>
	 * The {@link #segments} of the returned SegmentedReadData are all segments
	 * fully contained in the requested range.
	 *
	 * @param range a range in this ReadData
	 * @return a slice
	 * @throws N5Exception.N5IOException an exception
	 */
	@Override
	default SegmentedReadData slice(final Range range) throws N5Exception.N5IOException {
		return slice(range.offset(), range.length());
	}

	@Override
	SegmentedReadData materialize() throws N5Exception.N5IOException;
}
