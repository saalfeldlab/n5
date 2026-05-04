package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.transpose.TransposeCodec;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Used to create typed {@code DatasetCodec}s, which transform one {@link DataBlock} into another,
 * for example, by applying a transposition (@link {@link TransposeCodec}.
 */
@NameConfig.Prefix("data-codec") // TODO: is this Prefix correct?
public interface DatasetCodecInfo extends CodecInfo {

	DatasetCodec<?, ?> create(final DatasetAttributes attributes);
}
