package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(IdentityCodec.TYPE)
public class IdentityCodec implements Codec.BytesCodec {

	private static final long serialVersionUID = 8354269325800855621L;

	public static final String TYPE = "id";

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public ReadData decode(ReadData readData) {

		return readData;
	}

	@Override
	public ReadData encode(ReadData readData) {

		return readData;
	}
}
