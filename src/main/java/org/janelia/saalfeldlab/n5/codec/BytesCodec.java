package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class BytesCodec implements Codec {

	private static final long serialVersionUID = 3523505403978222360L;

	public static String TYPE = "bytes";

	protected final ByteOrder byteOrder;

	protected transient final byte[] array;

	public BytesCodec() {

		this(ByteOrder.LITTLE_ENDIAN);
	}

	public BytesCodec(final ByteOrder byteOrder) {

		this(byteOrder, 256);
	}

	public BytesCodec(final ByteOrder byteOrder, final int N) {

		this.byteOrder = byteOrder;
		this.array = new byte[N];
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// TODO not applicable for array -> bytes
		return in;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		// TODO not applicable for array -> bytes
		return out;
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
