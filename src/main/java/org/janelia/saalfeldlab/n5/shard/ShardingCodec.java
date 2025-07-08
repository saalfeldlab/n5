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
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.lang.reflect.Type;
import java.util.Objects;

@NameConfig.Name(ShardingCodec.TYPE)
public class ShardingCodec implements Codec.ArrayCodec {

	private static final long serialVersionUID = -5879797314954717810L;

	public static final String TYPE = "sharding_indexed";

	public static final String CHUNK_SHAPE_KEY = "chunk_shape";
	public static final String INDEX_LOCATION_KEY = "index_location";
	public static final String CODECS_KEY = "codecs";
	public static final String INDEX_CODECS_KEY = "index_codecs";
	private DatasetAttributes attributes = null;

	public enum IndexLocation {
		START, END
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

	/**
	 * Used via reflections by the NameConfig serializer.
	 */
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

		Objects.requireNonNull(codecs);
		if (codecs.length == 0)
			throw new IllegalArgumentException("Sharding Codec requires a single ArrayCodec. None found.");

		return (ArrayCodec)codecs[0];
	}

	public BytesCodec[] getCodecs() {

		Objects.requireNonNull(codecs);
		final BytesCodec[] bytesCodecs = new BytesCodec[codecs.length - 1];
		for (int i = 1; i < codecs.length; i++)
			bytesCodecs[i-1] = (BytesCodec)codecs[i];
		return bytesCodecs;
	}

	public DeterministicSizeCodec[] getIndexCodecs() {

		return indexCodecs;
	}

	@Override
	public long[] getPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		final long[] blockPosition = datablock.getGridPosition();
		return attributes.getShardPositionForBlock(blockPosition);
	}

	@Override
	public long[] getPositionForBlock(DatasetAttributes attributes, final long... blockPosition) {

		return attributes.getShardPositionForBlock(blockPosition);
	}

	@Override
	public void initialize(DatasetAttributes attributes, final BytesCodec[] codecs) {

		this.attributes = attributes;
		getArrayCodec().initialize(attributes, getCodecs());
	}

	@Override
	public <T> ReadData encode(DataBlock<T> dataBlock) {

		return getArrayCodec().encode(dataBlock);
	}

	@Override
	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException {

		final ReadData splitableReadData = readData.materialize();
		final long[] shardPosition = getPositionForBlock(attributes, gridPosition);
		final VirtualShard<T> shard = new VirtualShard<>(attributes, shardPosition, splitableReadData);
		final int[] relativeBlockPosition = shard.getRelativeBlockPosition(gridPosition);
		return shard.getBlock(relativeBlockPosition);
	}

	public ShardIndex createIndex(final DatasetAttributes attributes) {

		return new ShardIndex(attributes.getBlocksPerShard(), getIndexLocation(), getIndexCodecs());
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public static IndexLocationAdapter indexLocationAdapter = new IndexLocationAdapter();

	public static class IndexLocationAdapter implements JsonSerializer<IndexLocation>, JsonDeserializer<IndexLocation> {

		@Override
		public IndexLocation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (!json.isJsonPrimitive())
				return null;

			return IndexLocation.valueOf(json.getAsString().toUpperCase());
		}

		@Override
		public JsonElement serialize(IndexLocation src, Type typeOfSrc, JsonSerializationContext context) {

			return new JsonPrimitive(src.name().toLowerCase());
		}
	}

}
