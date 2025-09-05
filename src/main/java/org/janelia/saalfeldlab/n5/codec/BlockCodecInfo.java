package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Metadata and factory for a particular family of {@code BlockCodec}.
 * <p>
 * {@code BlockCodec}s encode {@link DataBlock}s into {@link ReadData} and
 * decode {@link ReadData} into {@link DataBlock}s.
 */
public interface BlockCodecInfo extends CodecInfo {

	<T> BlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs);

	default <T> BlockCodec<T> create(final DatasetAttributes attributes, final DataCodecInfo... codecInfos) {
		return create(attributes.getDataType(), attributes.getBlockSize(), codecInfos);
	}
}
