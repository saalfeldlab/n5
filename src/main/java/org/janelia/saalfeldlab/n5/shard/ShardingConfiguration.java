package org.janelia.saalfeldlab.n5.shard;

import java.lang.reflect.Type;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.codec.Codec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ShardingConfiguration {

	public static final String CHUNK_SHAPE_KEY = "chunk_shape";
	public static final String INDEX_LOCATION_KEY = "index_location";
	public static final String CODECS_KEY = "codecs";
	public static final String INDEX_CODECS_KEY = "index_codecs";

	public static enum IndexLocation {
		start, end
	};

	protected int[] blockSize;
	protected Codec[] codecs;
	protected Codec[] indexCodecs;
	protected IndexLocation indexLocation;

	public ShardingConfiguration(final int[] blockSize, final Codec[] codecs, final Codec[] indexCodecs,
			final IndexLocation indexLocation) {

		this.blockSize = blockSize;
		this.codecs = codecs;
		this.indexCodecs = indexCodecs;
		this.indexLocation = indexLocation;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public boolean areIndexesAtStart() {

		return indexLocation == IndexLocation.start;
	}

	public static class ShardingConfigurationAdapter
			implements JsonDeserializer<ShardingConfiguration>, JsonSerializer<ShardingConfiguration> {

		@Override
		public JsonElement serialize(ShardingConfiguration src, Type typeOfSrc, JsonSerializationContext context) {

			if( anyShardingCodecs(src.codecs) || anyShardingCodecs(src.indexCodecs))
				return JsonNull.INSTANCE;

			final JsonObject jsonObj = new JsonObject();
			jsonObj.add(CHUNK_SHAPE_KEY, context.serialize(src.blockSize));
			jsonObj.add(INDEX_LOCATION_KEY, context.serialize(src.indexLocation.toString()));
			jsonObj.add(CODECS_KEY, context.serialize(src.codecs));
			jsonObj.add(INDEX_CODECS_KEY, context.serialize(src.indexCodecs));

			return jsonObj;
		}

		@Override
		public ShardingConfiguration deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {

			return null;
		}

		public boolean anyShardingCodecs(final Codec[] codecs) {

			if (codecs == null)
				return false;

			return Arrays.stream(codecs).anyMatch(c -> {
				return (c instanceof ShardingCodec);
			});
		}

	}

}
