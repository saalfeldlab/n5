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
import org.janelia.saalfeldlab.n5.DatasetAttributes;
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

@NameConfig.Name(value = BytesCodec.TYPE)
public class BytesCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = 3282569607795127005L;

	public static final String TYPE = "bytes";

	@NameConfig.Parameter(value = "endian", optional = true)
	protected final ByteOrder byteOrder;

	public BytesCodec() {

		this(ByteOrder.LITTLE_ENDIAN);
	}

	public BytesCodec(final ByteOrder byteOrder) {

		this.byteOrder = byteOrder;
	}

	@Override
	public DataBlockInputStream decode(final DatasetAttributes attributes, final long[] gridPosition, InputStream in)
			throws IOException {

		return new DataBlockInputStream(in) {

			private int[] blockSize = attributes.getBlockSize();
			private int numElements = Arrays.stream(blockSize).reduce(1, (x, y) -> {
				return x * y;
			});

			@Override
			protected void beforeRead(int n) throws IOException {}

			@Override
			public DataBlock<?> allocateDataBlock() throws IOException {

				return attributes.getDataType().createDataBlock(blockSize, gridPosition, numElements);
			}

			@Override
			public DataInput getDataInput(final InputStream inputStream) {

				if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
					return new DataInputStream(inputStream);
				else
					return new LittleEndianDataInputStream(inputStream);
			}

		};
	}

	@Override
	public DataBlockOutputStream encode(final DatasetAttributes attributes, final DataBlock<?> dataBlock,
			final OutputStream out)
			throws IOException {

		return new DataBlockOutputStream(out) {

			@Override
			protected void beforeWrite(int n) throws IOException {}

			@Override
			public DataOutput getDataOutput(OutputStream outputStream) {

				if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
					return new DataOutputStream(outputStream);
				else
					return new LittleEndianDataOutputStream(outputStream);
			}
		};
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static final ByteOrderAdapter byteOrderAdapter = new ByteOrderAdapter();

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
