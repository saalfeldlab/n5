package org.janelia.saalfeldlab.n5.codec;


public class BytesCodec extends IdentityCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String ID = "bytes";

	protected final String name = ID;

	protected final String endian = "little";

	// TODO implement me

	@Override
	public String getName() {

		return name;
	}
}
