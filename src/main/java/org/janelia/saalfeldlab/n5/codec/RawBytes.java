package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import javax.annotation.Nullable;

@NameConfig.Name(value = RawBytes.TYPE)
public class RawBytes<T> implements Codec.ArrayCodec<T> {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	private DataBlockCodec<T> dataBlockCodec;
	private DatasetAttributes attributes;

	public RawBytes() {

		this(ByteOrder.BIG_ENDIAN);
	}

	public RawBytes(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	@Nullable
	public ByteOrder getByteOrder() {
		return byteOrder;
	}

	@Override public void setDatasetAttributes(DatasetAttributes attributes, BytesCodec... codecs) {
		ensureValidByteOrder(attributes.getDataType(), getByteOrder());
		this.attributes = attributes;
		final BytesCodec[] byteCodecs = codecs == null ? attributes.getCodecs() : codecs;
		this.dataBlockCodec = RawBlockCodecs.createDataBlockCodec(attributes.getDataType(), getByteOrder(), attributes.getBlockSize(), byteCodecs);
	}

	@Override public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException {

		return dataBlockCodec.decode(readData, gridPosition);
	}

	@Override public ReadData encode(DataBlock<T> dataBlock) throws IOException {

		return dataBlockCodec.encode(dataBlock);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static final ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

	public static void ensureValidByteOrder(final DataType dataType, @Nullable final ByteOrder byteOrder) {

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
