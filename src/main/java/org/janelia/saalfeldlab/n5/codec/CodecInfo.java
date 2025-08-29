package org.janelia.saalfeldlab.n5.codec;

import java.io.Serializable;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * {@code CodecInfo}s can encode and decode {@link ReadData} objects.
 * <p>
 * Modeled after <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/index.html">Codecs</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface CodecInfo extends Serializable {

	String getType();
}
