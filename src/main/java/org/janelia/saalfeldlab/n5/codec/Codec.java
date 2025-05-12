package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Interface representing a filter can encode a {@link OutputStream}s when writing data, and decode
 * the {@link InputStream}s when reading data.
 * <p>
 * Modeled after <a href="https://zarr.readthedocs.io/en/v2.0.1/api/codecs.html">Filters</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface Codec extends Serializable {

	interface BytesCodec extends Codec {

		// --------------------------------------------------
		//

		/**
		 * Decode the given {@code readData}.
		 * <p>
		 * The returned decoded {@code ReadData} reports {@link ReadData#length()
		 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
		 * depending on the {@code BytesCodec} implementation.
		 *
		 * @param readData data to decode
		 * @return decoded ReadData
		 * @throws IOException if any I/O error occurs
		 */
		ReadData decode(ReadData readData) throws IOException;

		/**
		 * Encode the given {@code readData}.
		 * <p>
		 * Encoding may be lazy or eager, depending on the {@code BytesCodec}
		 * implementation.
		 *
		 * @param readData data to encode
		 * @return encoded ReadData
		 * @throws IOException if any I/O error occurs
		 */
		ReadData encode(ReadData readData) throws IOException;

	}

	interface ArrayCodec<T> extends DeterministicSizeCodec, DataBlockCodec<T> {

		default long[] getPositionForBlock(final DatasetAttributes attributes, final DataBlock<?> datablock) {

			return datablock.getGridPosition();
		}

		default long[] getPositionForBlock(final DatasetAttributes attributes, final long... blockPosition) {

			return blockPosition;
		}

		void setDatasetAttributes(final DatasetAttributes attributes, final BytesCodec... codecs);

		@Override default long encodedSize(long size) {

			return size;
		}

		@Override default long decodedSize(long size) {

			return size;
		}
	}

	String getType();
}

