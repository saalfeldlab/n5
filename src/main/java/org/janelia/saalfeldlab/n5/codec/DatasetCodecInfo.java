package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
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

	/**
	 * Creates the codec for a given position in a chain of dataset codecs.
	 * <p>
	 * A dataset codec is not necessarily applied to blocks of the dataset's own data
	 * type: an earlier codec in the chain may have changed it. {@code sourceDataType}
	 * is the type of the blocks this codec will actually be handed when encoding.
	 * <p>
	 * The default implementation ignores it and delegates to {@link #create(DatasetAttributes)},
	 * which is correct only for a codec that is first in the chain. Implementations whose
	 * behaviour depends on the input type should override this.
	 *
	 * @param attributes
	 *            the dataset attributes
	 * @param sourceDataType
	 *            the data type of the blocks passed to the returned codec's encode method
	 * @return the codec
	 */
	default DatasetCodec<?, ?> create(final DatasetAttributes attributes, final DataType sourceDataType) {

		return create(attributes);
	}

	/**
	 * The data type of the blocks this codec produces when encoding blocks of the given
	 * type.
	 * <p>
	 * The default implementation is the identity, which is correct for every
	 * type-preserving codec. Only a codec that converts between data types (such as
	 * {@code cast_value}) needs to override it.
	 *
	 * @param dataType
	 *            the data type of the blocks passed to encode
	 * @return the data type of the blocks returned by encode
	 */
	default DataType encodedDataType(final DataType dataType) {

		return dataType;
	}
}
