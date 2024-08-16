package org.janelia.saalfeldlab.n5.shard;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;

@NameConfig.Name(ShardingCodec.TYPE)
public class ShardingCodec implements Codec.BytesToBytes { //TODO Caleb: should be ArrayToBytes

	private static final long serialVersionUID = -5879797314954717810L;

	public static final String TYPE = "sharding_indexed";

	public final static String CHUNK_SHAPE_KEY = "chunk_shape";
	public static final String INDEX_LOCATION_KEY = "index_location";
	public static final String CODECS_KEY = "codecs";
	public static final String INDEX_CODECS_KEY = "index_codecs";

	public enum IndexLocation {
		START, END
	}

	@NameConfig.Parameter(CHUNK_SHAPE_KEY)
	private final int[] blockSize;

	@NameConfig.Parameter(CODECS_KEY)
	private final Codec[] codecs;

	@NameConfig.Parameter(INDEX_CODECS_KEY)
	private final Codec[] indexCodecs;

	@NameConfig.Parameter(INDEX_LOCATION_KEY)
	private final IndexLocation indexLocation;

	private ShardingCodec() {

		blockSize = null;
		codecs = null;
		indexCodecs = null;
		indexLocation = null;
	}

	public ShardingCodec(
			final int[] blockSize,
			final Codec[] codecs,
			final Codec[] indexCodecs,
			final IndexLocation indexLocation) {

		this.blockSize = blockSize;
		this.codecs = codecs;
		this.indexCodecs = indexCodecs;
		this.indexLocation = indexLocation;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public IndexLocation getIndexLocation() {

		return indexLocation;
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		// TODO Auto-generated method stub
		// This method actually makes no sense for a sharding codec
		return in;
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		// TODO Auto-generated method stub
		// This method actually makes no sense for a sharding codec
		return out;
	}

	public static boolean isShardingCodec(final Codec codec) {

		return codec instanceof ShardingCodec;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static IndexLocationAdapter indexLocationAdapter = new IndexLocationAdapter();

	public static class IndexLocationAdapter implements JsonSerializer<IndexLocation>, JsonDeserializer<IndexLocation> {

		@Override public IndexLocation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (!json.isJsonPrimitive()) return null;

			return IndexLocation.valueOf(json.getAsString().toUpperCase());
		}

		@Override public JsonElement serialize(IndexLocation src, Type typeOfSrc, JsonSerializationContext context) {

			return new JsonPrimitive(src.name().toLowerCase());
		}
	}

}
