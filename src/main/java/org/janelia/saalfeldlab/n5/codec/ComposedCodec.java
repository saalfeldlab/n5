package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.janelia.saalfeldlab.n5.serialization.N5NameConfig;

/**
 * A {@link ByteStreamCodec} that is composition of a collection of codecs.
 */
@N5NameConfig.Type("composed")
@N5NameConfig.Prefix("codec")
public class ComposedCodec implements ByteStreamCodec {

	private static final long serialVersionUID = 5068349140842235924L;

	private final ByteStreamCodec[] codecs;

	public ComposedCodec(final ByteStreamCodec... codecs) {

		this.codecs = codecs;
	}

	private ComposedCodec() {

		this(null);
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// note that decoding is in reverse order
		InputStream decoded = in;
		for (int i = codecs.length - 1; i >= 0; i--)
			decoded = codecs[i].decode(decoded);

		return decoded;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		OutputStream encoded = out;
		for (int i = 0; i < codecs.length; i++)
			encoded = codecs[i].encode(encoded);

		return encoded;
	}

}
