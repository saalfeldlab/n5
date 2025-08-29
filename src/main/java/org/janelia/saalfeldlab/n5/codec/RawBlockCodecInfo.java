package org.janelia.saalfeldlab.n5.codec;

import java.nio.ByteOrder;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
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
	public <T> BlockCodec<T> create(final DatasetAttributes attributes, final DataCodec... dataCodecs) {
		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		return RawBlockCodecs.create(attributes.getDataType(), byteOrder, attributes.getBlockSize(), DataCodec.concatenate(dataCodecs));
	}

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

	public static ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

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
