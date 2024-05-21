package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IdentityCodec implements Codec {

	private static final long serialVersionUID = 8354269325800855621L;

	@Override
	public InputStream decode(InputStream in) throws IOException {

		return in;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		return out;
	}

}
