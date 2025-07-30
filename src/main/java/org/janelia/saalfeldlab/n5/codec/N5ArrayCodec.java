package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5ArrayCodec.TYPE)
public class N5ArrayCodec implements ArrayCodec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public <T> DataBlockSerializer<T> initialize(final DatasetAttributes attributes, final BytesCodec... bytesCodecs) {
		return N5DataBlockSerializers.create(attributes.getDataType(), BytesCodec.concatenate(bytesCodecs));
	}

}
