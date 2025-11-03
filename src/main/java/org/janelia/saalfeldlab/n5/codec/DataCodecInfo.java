package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * {@code DataCodec}s transform one {@link ReadData} into another,
 * for example, compressing it.
 */
@NameConfig.Prefix("data-codec")
public interface DataCodecInfo extends CodecInfo {

	DataCodec create();
}
