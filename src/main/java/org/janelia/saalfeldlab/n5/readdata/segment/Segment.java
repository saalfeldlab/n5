package org.janelia.saalfeldlab.n5.readdata.segment;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A particular segment in a source {@code ReadData}.
 */
public interface Segment {

	SegmentedReadData source();
}
