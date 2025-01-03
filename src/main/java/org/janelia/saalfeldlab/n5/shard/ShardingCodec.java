package org.janelia.saalfeldlab.n5.shard;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;

@NameConfig.Name(ShardingCodec.TYPE)
public class ShardingCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = -5879797314954717810L;

	public static final String TYPE = "sharding_indexed";

	public static final String CHUNK_SHAPE_KEY = "chunk_shape";
	public static final String INDEX_LOCATION_KEY = "index_location";
	public static final String CODECS_KEY = "codecs";
	public static final String INDEX_CODECS_KEY = "index_codecs";

	public enum IndexLocation {
		START, END;
	}

	@N5Annotations.ReverseArray // TODO need to reverse for zarr, not for n5
	@NameConfig.Parameter(CHUNK_SHAPE_KEY)
	private final int[] blockSize;

	@NameConfig.Parameter(CODECS_KEY)
	private final Codec[] codecs;

	@NameConfig.Parameter(INDEX_CODECS_KEY)
	private final DeterministicSizeCodec[] indexCodecs;

	@NameConfig.Parameter(value = INDEX_LOCATION_KEY, optional = true)
	private final IndexLocation indexLocation;

	@SuppressWarnings("unused")
	private ShardingCodec() {

		blockSize = null;
		codecs = null;
		indexCodecs = null;
		indexLocation = IndexLocation.END;
	}

	public ShardingCodec(
			final int[] blockSize,
			final Codec[] codecs,
			final DeterministicSizeCodec[] indexCodecs,
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

	public ArrayCodec getArrayCodec() {

		return (Codec.ArrayCodec)codecs[0];
	}

	public BytesCodec[] getCodecs() {

		if (codecs.length == 1)
			return new BytesCodec[]{};

		final BytesCodec[] bytesCodecs = new BytesCodec[codecs.length - 1];
		System.arraycopy(codecs, 1, bytesCodecs, 0, bytesCodecs.length);
		return bytesCodecs;
	}

	public DeterministicSizeCodec[] getIndexCodecs() {

		return indexCodecs;
	}

	@Override public DataBlockInputStream decode(DatasetAttributes attributes, long[] gridPosition, InputStream in) throws IOException {

		return getArrayCodec().decode(attributes, gridPosition, in);
	}

	@Override public DataBlockOutputStream encode(DatasetAttributes attributes, DataBlock<?> datablock, OutputStream out) throws IOException {

		return getArrayCodec().encode(attributes, datablock, out);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static IndexLocationAdapter indexLocationAdapter = new IndexLocationAdapter();

	public static class IndexLocationAdapter implements JsonSerializer<IndexLocation>, JsonDeserializer<IndexLocation> {

		@Override public IndexLocation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (!json.isJsonPrimitive())
				return null;

			return IndexLocation.valueOf(json.getAsString().toUpperCase());
		}

		@Override public JsonElement serialize(IndexLocation src, Type typeOfSrc, JsonSerializationContext context) {

			return new JsonPrimitive(src.name().toLowerCase());
		}
	}

}
