package org.janelia.saalfeldlab.n5.readdata.segment;

import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

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
		return DefaultSegmentedReadData.wrap(readData);
	}

	/**
	 * Wrap {@code readData} and create segments at the given locations. The
	 * order of segments in the returned {@link SegmentsAndData#segments()} list
	 * matches the order of the given {@code locations} (while the segments in the
	 * {@link SegmentsAndData#data()} are ordered by offset).
	 */
	static SegmentsAndData wrap(ReadData readData, SegmentLocation... locations) {
		return wrap(readData, Arrays.asList(locations));
	}

	/**
	 * Wrap {@code readData} and create segments at the given locations. The
	 * order of segments in the returned {@link SegmentsAndData#segments()} list
	 * matches the order of the given {@code locations} (while the segments in the
	 * {@link SegmentsAndData#data()} are ordered by offset).
	 */
	static SegmentsAndData wrap(ReadData readData, List<SegmentLocation> locations) {
		return DefaultSegmentedReadData.wrap(readData, locations);
	}

	static SegmentedReadData concatenate(List<SegmentedReadData> readDatas) {
		return new Concatenate(readDatas);
	}



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
	 *
	 * @throws IllegalArgumentException
	 * 		if the segment is not contained in this ReadData
	 */
	SegmentLocation location(Segment segment) throws IllegalArgumentException;

	/**
	 * @return all segments contained in this {@code ReadData}.
	 */
	// Order is the same as the SegmentLocations given at construction
	List<? extends Segment> segments();

	@Override
	default SegmentedReadData limit(final long length) throws N5Exception.N5IOException {
		return slice(0, length);
	}

	/**
	 * Return a {@code SegmentedReadData} wrapping a slice containing exactly the given segment.
	 *
	 * @param segment
	 *
	 * @return
	 *
	 * @throws IllegalArgumentException
	 * 		if the segment is not contained in this ReadData
	 * @throws N5Exception.N5IOException
	 */
	SegmentedReadData slice(Segment segment) throws IllegalArgumentException, N5Exception.N5IOException;

	// TODO: has all segments fully contained in requested slice.
	@Override
	SegmentedReadData slice(final long offset, final long length) throws N5Exception.N5IOException;

	@Override
	SegmentedReadData materialize() throws N5Exception.N5IOException;
}
