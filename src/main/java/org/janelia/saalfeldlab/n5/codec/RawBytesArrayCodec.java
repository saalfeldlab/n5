package org.janelia.saalfeldlab.n5.codec;

import java.nio.ByteOrder;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;


@NameConfig.Name(value = RawBytesArrayCodec.TYPE)
public class RawBytesArrayCodec implements ArrayCodec {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	private DatasetAttributes attributes;

	private BytesCodec bytesCodec;

	public RawBytesArrayCodec() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBytesArrayCodec(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override
	public void initialize(DatasetAttributes attributes, BytesCodec[] bytesCodecs) {
		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		this.attributes = attributes;
		this.bytesCodec = BytesCodec.concatenate(bytesCodecs);
	}

	private <T> DataBlockSerializer<T> getDataBlockCodec() {

		return RawDataBlockSerializers.createDataBlockCodec( attributes.getDataType(), getByteOrder(), attributes.getBlockSize(), bytesCodec );
	}

	@Override
	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) {
		return this.<T>getDataBlockCodec().decode(readData, gridPosition);
	}


	@Override
	public <T> ReadData encode(DataBlock<T> dataBlock) {

		return this.<T>getDataBlockCodec().encode(dataBlock);
	}

	@Override
	public String getType() {

		return TYPE;
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
