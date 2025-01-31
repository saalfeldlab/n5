package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public interface BytesCodec {

	/**
	 * Decode an {@link InputStream}.
	 *
	 * @param in
	 *            input stream
	 * @return the decoded input stream
	 */
	InputStream decode(InputStream in) throws IOException;

	/**
	 * Encode an {@link OutputStream}.
	 *
	 * @param out
	 *            the output stream
	 * @return the encoded output stream
	 */
	OutputStream encode(OutputStream out) throws IOException;

	/**
	 * TODO javadoc
	 *
	 * @param readData
	 * @return
	 */
	ReadData encode(ReadData readData) throws IOException;

	/**
	 * TODO javadoc
	 *
	 * @param readData
	 * @return
	 */
	default ReadData decode(ReadData readData) {
		throw new UnsupportedOperationException();
	}
}
