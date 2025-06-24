package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.Serializable;

import static org.janelia.saalfeldlab.n5.N5Exception.*;

/**
 * {@code Codec}s can encode and decode {@link ReadData} objects.
 * <p>
 * Modeled after <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/index.html">Codecs</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface Codec extends Serializable {

	String getType();

	/**
	 * {@code BytesCodec}s transform one {@link ReadData} into another,
	 * for example, compressing it.
	 */
	interface BytesCodec extends Codec {

		/**
		 * Decode the given {@link ReadData}.
		 * <p>
		 * The returned decoded {@code ReadData} reports {@link ReadData#length()
		 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
		 * depending on the {@code BytesCodec} implementation.
		 *
		 * @param readData data to decode
		 * @return decoded ReadData
		 * @throws IOException if any I/O error occurs
		 */
		ReadData decode(ReadData readData) throws N5IOException;

		/**
		 * Encode the given {@link ReadData}.
		 * <p>
		 * Encoding may be lazy or eager, depending on the {@code BytesCodec}
		 * implementation.
		 *
		 * @param readData data to encode
		 * @return encoded ReadData
		 * @throws IOException if any I/O error occurs
		 */
		ReadData encode(ReadData readData) throws N5IOException;

	}

	/**
	 * {@code ArrayCodec}s encode {@link DataBlock}s into {@link ReadData} and
	 * decode {@link ReadData} into {@link DataBlock}s.
	 */
	interface ArrayCodec extends DeterministicSizeCodec {

		<T> DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException;

		<T> ReadData encode(DataBlock<T> dataBlock) throws N5IOException;

		default long[] getPositionForBlock(final DatasetAttributes attributes, final DataBlock<?> datablock) {

			return datablock.getGridPosition();
		}

		default long[] getPositionForBlock(final DatasetAttributes attributes, final long... blockPosition) {

			return blockPosition;
		}

		void initialize(final DatasetAttributes attributes, final BytesCodec... codecs);

		@Override default long encodedSize(long size) {

			return size;
		}

		@Override default long decodedSize(long size) {

			return size;
		}
	}
}

