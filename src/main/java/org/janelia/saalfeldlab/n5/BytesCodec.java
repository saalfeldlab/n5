package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public interface BytesCodec {

	/**
	 * Decode the given {@code readData}.
	 * <p>
	 * The returned decoded {@code ReadData} reports {@link ReadData#length()
	 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
	 * depending on the {@code BytesCodec} implementation.
	 *
	 * @param readData
	 * 		data to decode
	 * @param decodedLength
	 * 		length of the decoded data (-1 if unknown)
	 *
	 * @return decoded ReadData
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 */
	ReadData decode(ReadData readData, int decodedLength) throws IOException;

	/**
	 * Encode the given {@code readData}.
	 * <p>
	 * Encoding may be lazy or eager, depending on the {@code BytesCodec}
	 * implementation.
	 *
	 * @param readData
	 * 		data to encode
	 *
	 * @return encoded ReadData
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 */
	ReadData encode(ReadData readData) throws IOException;
}
