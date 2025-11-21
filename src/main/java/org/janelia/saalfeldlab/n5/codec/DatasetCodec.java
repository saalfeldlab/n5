package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * A Codec that transforms the contents of a {@link DataBlock}.
 * <p>
 * This class is N5's analogue to Zarr's array -> array codec.
 */
public interface DatasetCodec {

	// TODO Name ideas:
	// "ImageCodec"?
	// BlockTransformationCodec

	DataBlock<?> encode(DataBlock<?> block) throws N5IOException;

	DataBlock<?> decode(DataBlock<?> dataBlock) throws N5IOException;

}
