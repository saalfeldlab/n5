package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Used to create {@code DataCodec}s, which transform one {@link ReadData} into another,
 * for example, applying compression.
 */
@NameConfig.Prefix("data-codec")
public interface DataCodecInfo extends CodecInfo {

	DataCodec create();
}
