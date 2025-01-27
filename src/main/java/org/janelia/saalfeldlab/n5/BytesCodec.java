package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
}
