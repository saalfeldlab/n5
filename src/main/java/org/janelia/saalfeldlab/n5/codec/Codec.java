package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Interface representing a filter can encode a {@link OutputStream}s when writing data, and decode
 * the {@link InputStream}s when reading data.
 *
 * Modeled after <a href="https://zarr.readthedocs.io/en/v2.0.1/api/codecs.html">Filters</a> in
 * Zarr.
 */
public interface Codec extends Serializable {

	/**
	 * Decode an {@link InputStream}.
	 *
	 * @param in
	 *            input stream
	 * @return the decoded input stream
	 */
	public InputStream decode(InputStream in) throws IOException;

	/**
	 * Encode an {@link OutputStream}.
	 *
	 * @param out
	 *            the output stream
	 * @return the encoded output stream
	 */
	public OutputStream encode(OutputStream out) throws IOException;

	public String getName();

}
