package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@NameConfig.Name(IdentityCodec.TYPE)
public class IdentityCodec implements Codec {

	private static final long serialVersionUID = 8354269325800855621L;

	protected static final String TYPE = "id";

	@Override
	public InputStream decode(InputStream in) throws IOException {

		return in;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		return out;
	}

	@Override
	public String getType() {

		return TYPE;
	}

}
