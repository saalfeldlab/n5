package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Codec} that is composition of a collection of codecs.
 */
public class ComposedCodec implements Codec.BytesToBytes { //TODO Caleb: Remove?

	private static final long serialVersionUID = 5068349140842235924L;

	protected static final String TYPE = "composed";

	private final Codec[] codecs;

	public ComposedCodec(final Codec... codec) {

		this.codecs = codec;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// note that decoding is in reverse order
		InputStream decoded = in;
		for (int i = codecs.length - 1; i >= 0; i--){}
//			decoded = codecs[i].decode(decoded);

		return decoded;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		OutputStream encoded = out;
		for (int i = 0; i < codecs.length; i++){}
//			encoded = codecs[i].encode(encoded);

		return encoded;
	}

}
