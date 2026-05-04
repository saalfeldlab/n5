package org.janelia.saalfeldlab.n5.codec;

import java.io.Serializable;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * {@code CodecInfo}s are an untyped semantic layer for {@link BlockCodec}s, {@link DataCodec}s, and {@link DatasetCodec}s.
 * <p>
 * Modeled after <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/index.html">Codecs</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface CodecInfo extends Serializable {

	String getType();
}
