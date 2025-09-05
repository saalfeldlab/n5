package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = N5BlockCodecInfo.TYPE)
public class N5BlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 3523505403978222360L;

	public static final String TYPE = "n5bytes";

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecInfos) {
		return N5BlockCodecs.create(dataType, DataCodec.create(codecInfos));
	}
}
