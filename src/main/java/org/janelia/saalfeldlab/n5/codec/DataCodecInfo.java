package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * {@code DataCodec}s transform one {@link ReadData} into another,
 * for example, compressing it.
 */
public interface DataCodecInfo extends CodecInfo {

	DataCodec create();
}
