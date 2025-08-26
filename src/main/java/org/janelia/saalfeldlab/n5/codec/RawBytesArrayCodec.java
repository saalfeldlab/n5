package org.janelia.saalfeldlab.n5.codec;

import java.nio.ByteOrder;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;


@NameConfig.Name(value = RawBytesArrayCodec.TYPE)
public class RawBytesArrayCodec implements BlockCodecInfo {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "rawbytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	private final ByteOrder byteOrder;

	public RawBytesArrayCodec() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBytesArrayCodec(final ByteOrder byteOrder) {

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
	public <T> DataBlockSerializer<T> create(final DatasetAttributes attributes, final BytesCodec... bytesCodecs) {
		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		return RawDataBlockSerializers.create(attributes.getDataType(), byteOrder, attributes.getBlockSize(), BytesCodec.concatenate(bytesCodecs));
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
