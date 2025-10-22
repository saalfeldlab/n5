package org.janelia.saalfeldlab.n5.readdata.segment;

/**
 * A particular segment in a source {@link SegmentedReadData}.
 */
public interface Segment {

	/**
	 * Returns the {@code SegmentedReadData} on which this segment is originally
	 * defined. (The segment is tracked through slices and concatenations, but
	 * the source will remain the same.)
	 * <p>
	 * This is mostly just used internally to make {@code SegmentedReadData}
	 * implementations easier. The only real use for {@code source()} outside of
	 * that is to get a {@code ReadData} containing exactly this segment, using
	 * {@code segment.source().slice(segment)}.
	 *
	 * @return the {@code SegmentedReadData} on which this segment is originally defined
	 */
	SegmentedReadData source();
}
