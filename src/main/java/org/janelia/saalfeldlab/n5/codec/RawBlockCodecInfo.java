package org.janelia.saalfeldlab.n5.codec;

import java.nio.ByteOrder;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;


@NameConfig.Name(value = RawBlockCodecInfo.TYPE)
public class RawBlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "rawbytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	private final ByteOrder byteOrder;

	public RawBlockCodecInfo() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBlockCodecInfo(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override
	public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecInfos) {
		ensureValidByteOrder(dataType, getByteOrder());
		return RawBlockCodecs.create(dataType, byteOrder, blockSize, DataCodec.create(codecInfos));
	}

	private static void ensureValidByteOrder(final DataType dataType, final ByteOrder byteOrder) {

		switch (dataType) {
		case INT8:
		case UINT8:
		case STRING:
		case OBJECT:
			return;
		}

		if (byteOrder == null)
			throw new IllegalArgumentException("DataType (" + dataType + ") requires ByteOrder, but was null");
	}
}
