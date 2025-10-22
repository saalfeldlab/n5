package org.janelia.saalfeldlab.n5.readdata.segment;

/**
 * A particular segment in a source {@link SegmentedReadData}.
 */
public interface Segment {

	SegmentedReadData source();
}
