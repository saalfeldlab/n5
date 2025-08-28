package org.janelia.saalfeldlab.n5.codec;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Metadata and factory for a particular family of {@code BlockCodec}.
 * <p>
 * {@code BlockCodec}s encode {@link DataBlock}s into {@link ReadData} and
 * decode {@link ReadData} into {@link DataBlock}s.
 */
public interface BlockCodecInfo extends CodecInfo {

	<T> BlockCodec<T> create(DatasetAttributes attributes, DataCodec... codecs);

	default <T> BlockCodec<T> create(final DatasetAttributes attributes, final DataCodecInfo... codecInfos) {
		final DataCodec[] codecs = new DataCodec[codecInfos.length];
		Arrays.setAll(codecs, i -> codecInfos[i].create());
		return create(attributes, codecs);
	}

	// TODO: Should we have both create() signatures?
}
