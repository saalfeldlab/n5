package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(IdentityCodec.TYPE)
@NameConfig.Prefix("codec")
public class IdentityCodec implements Codec {

	private static final long serialVersionUID = 8354269325800855621L;

	public static final String TYPE = "id";

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
