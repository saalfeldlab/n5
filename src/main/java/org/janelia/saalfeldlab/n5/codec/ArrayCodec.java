package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * {@code ArrayCodec}s encode {@link DataBlock}s into {@link ReadData} and
 * decode {@link ReadData} into {@link DataBlock}s.
 */
public interface ArrayCodec extends CodecInfo {

	<T> DataBlockSerializer<T> initialize(final DatasetAttributes attributes, final BytesCodec... codecs);

}
