package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Codec} that is composition of a collection of codecs.
 */
public class ComposedCodec implements Codec {

	private static final long serialVersionUID = 5068349140842235924L;
	private final Codec[] filters;

	protected String id = "composed";

	public ComposedCodec(final Codec... filters) {

		this.filters = filters;
	}

	@Override
	public String getId() {

		return id;
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// note that decoding is in reverse order
		InputStream decoded = in;
		for (int i = filters.length - 1; i >= 0; i--)
			decoded = filters[i].decode(decoded);

		return decoded;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		OutputStream encoded = out;
		for (int i = 0; i < filters.length; i++)
			encoded = filters[i].encode(encoded);

		return encoded;
	}

}
