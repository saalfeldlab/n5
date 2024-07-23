package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;

import org.janelia.saalfeldlab.n5.codec.Codec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ShardingCodec implements Codec {

	private static final long serialVersionUID = -5879797314954717810L;

	public static final String ID = "sharding_indexed";

	private final ShardingConfiguration configuration;

	private final String name = ID;

	public ShardingCodec(ShardingConfiguration configuration) {

		this.configuration = configuration;
	}

	public ShardingConfiguration getConfiguration() {

		return configuration;
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// TODO Auto-generated method stub
		return in;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		// TODO Auto-generated method stub
		return out;
	}

	@Override
	public String getName() {

		return name;
	}

	public static boolean isShardingCodec(final Codec codec) {

		return codec instanceof ShardingCodec;
	}

	// public static void TypeAd
	public static class ShardingCodecAdapter implements JsonDeserializer<ShardingCodec>, JsonSerializer<ShardingCodec> {

		@Override
		public JsonElement serialize(ShardingCodec src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject jsonObj = new JsonObject();

			jsonObj.addProperty("name", ShardingCodec.ID);
			// context.serialize(typeOfSrc);

			return jsonObj;
		}

		@Override
		public ShardingCodec deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {

			return null;
		}

	}

}
