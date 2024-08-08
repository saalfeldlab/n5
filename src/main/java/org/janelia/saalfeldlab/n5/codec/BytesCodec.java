package org.janelia.saalfeldlab.n5.codec;


public class BytesCodec extends IdentityCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "bytes";

	private final String endian;

	public BytesCodec() {

		this("little");
	}

	public BytesCodec(final String endian) {

		this.endian = endian;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public String getEndian() {

		return endian;
	}
}
