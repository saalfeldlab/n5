package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * {@code BytesCodec}s transform one {@link ReadData} into another,
 * for example, compressing it.
 */
public interface BytesCodec extends CodecInfo {

	/**
	 * Decode the given {@link ReadData}.
	 * <p>
	 * The returned decoded {@code ReadData} reports {@link ReadData#length()
	 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
	 * depending on the {@code BytesCodec} implementation.
	 *
	 * @param readData
	 * 		data to decode
	 *
	 * @return decoded ReadData
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData decode(ReadData readData) throws N5IOException;

	/**
	 * Encode the given {@link ReadData}.
	 * <p>
	 * Encoding may be lazy or eager, depending on the {@code BytesCodec}
	 * implementation.
	 *
	 * @param readData
	 * 		data to encode
	 *
	 * @return encoded ReadData
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData encode(ReadData readData) throws N5IOException;

	/**
	 * Create a {@code BytesCodec} that sequentially applies {@code codecs} in
	 * the given order for encoding, and in reverse order for decoding.
	 *
	 * @param codecs
	 *            a list of BytesCodecs
	 * @return the concatenated BytesCodec
	 */
	static BytesCodec concatenate(final BytesCodec... codecs) {

		if (codecs == null)
			throw new NullPointerException();

		if (codecs.length == 1)
			return codecs[0];

		return new ConcatenatedBytesCodec(codecs);
	}
}
