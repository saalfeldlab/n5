package org.janelia.saalfeldlab.n5.codec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.nio.ByteOrder;


@NameConfig.Name(value = RawBytes.TYPE)
public class RawBytes implements Codec.ArrayCodec {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	private DatasetAttributes attributes;

	private BytesCodec bytesCodec;

	public RawBytes() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBytes(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override public void initialize(DatasetAttributes attributes, BytesCodec[] byteCodecs) {
		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		this.attributes = attributes;
		this.bytesCodec = new ConcatenatedBytesCodec(byteCodecs);
	}
	
	private <T> DataBlockCodec<T> getDataBlockCodec() {

		return RawBlockCodecs.createDataBlockCodec( attributes.getDataType(), getByteOrder(), attributes.getBlockSize(), bytesCodec );
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

	public static final RawBytes.ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

	public static void ensureValidByteOrder(final DataType dataType, final ByteOrder byteOrder) {

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

	public static class ByteOrderAdapter implements JsonDeserializer<ByteOrder>, JsonSerializer<ByteOrder> {

		@Override
		public JsonElement serialize(ByteOrder src, java.lang.reflect.Type typeOfSrc,
				JsonSerializationContext context) {

			if (src.equals(ByteOrder.LITTLE_ENDIAN))
				return new JsonPrimitive("little");
			else
				return new JsonPrimitive("big");
		}

		@Override
		public ByteOrder deserialize(JsonElement json, java.lang.reflect.Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {

			if (json.getAsString().equals("little"))
				return ByteOrder.LITTLE_ENDIAN;
			if (json.getAsString().equals("big"))
				return ByteOrder.BIG_ENDIAN;

			return null;
		}

	}

}
