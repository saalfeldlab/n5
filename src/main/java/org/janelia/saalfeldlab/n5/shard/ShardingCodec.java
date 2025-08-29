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
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.IndexCodecAdapter;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.lang.reflect.Type;
import java.util.Objects;

@NameConfig.Name(ShardingCodec.TYPE)
public class ShardingCodec implements BlockCodecInfo {

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
	private final CodecInfo[] codecs;

	@NameConfig.Parameter(INDEX_CODECS_KEY)
	private final IndexCodecAdapter indexCodecs;

	@NameConfig.Parameter(value = INDEX_LOCATION_KEY, optional = true)
	private final IndexLocation indexLocation;

	protected BlockCodec<?> dataBlockSerializer = null;

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
			final CodecInfo[] codecs,
			final IndexCodecAdapter indexCodecs,
			final IndexLocation indexLocation) {

		this.blockSize = blockSize;
		this.codecs = codecs;
		this.indexCodecs = indexCodecs;
		this.indexLocation = indexLocation;
	}

	public ShardingCodec(
			final int[] blockSize,
			final CodecInfo[] codecs,
			final CodecInfo[] indexCodecs,
			final IndexLocation indexLocation) {

		this(blockSize, codecs, IndexCodecAdapter.create(indexCodecs), indexLocation);
	}

	public IndexLocation getIndexLocation() {

		return indexLocation;
	}

	public BlockCodecInfo getArrayCodec() {

		Objects.requireNonNull(codecs);
		if (codecs.length == 0)
			throw new IllegalArgumentException("Sharding CodecInfo requires a single BlockCodecInfo. None found.");

		return (BlockCodecInfo)codecs[0];
	}
	public <T> BlockCodec<T> getDataBlockSerializer() {

		return (BlockCodec<T>)dataBlockSerializer;
	}

	public DataCodec[] getCodecs() {

		Objects.requireNonNull(codecs);
		final DataCodec[] bytesCodecs = new DataCodec[codecs.length - 1];
		for (int i = 1; i < codecs.length; i++)
			bytesCodecs[i-1] = (DataCodec)codecs[i];
		return bytesCodecs;
	}

	public IndexCodecAdapter getIndexCodecAdapter() {

		return indexCodecs;
	}

	@Override
	public long[] getKeyPositionForBlock(DatasetAttributes attributes, DataBlock<?> datablock) {

		final long[] blockPosition = datablock.getGridPosition();
		return attributes.getShardPositionForBlock(blockPosition);
	}

	@Override
	public long[] getKeyPositionForBlock(DatasetAttributes attributes, final long... blockPosition) {

		return attributes.getShardPositionForBlock(blockPosition);
	}

	@Override
	public <T> BlockCodec<T> create(DatasetAttributes attributes, final DataCodec[] codecs) {

		this.attributes = attributes;
		this.dataBlockSerializer = getArrayCodec().<T>create(attributes, getCodecs());
		return ((BlockCodec<T>)dataBlockSerializer);
	}

	public <T> ReadData encode(DataBlock<T> dataBlock) {

		return this.<T>getDataBlockSerializer().encode(dataBlock);
	}

	public <T> DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException {

		final ReadData splitableReadData = readData.materialize();
		final long[] shardPosition = getKeyPositionForBlock(attributes, gridPosition);
		final VirtualShard<T> shard = new VirtualShard<>(attributes, shardPosition, splitableReadData);
		final int[] relativeBlockPosition = shard.getRelativeBlockPosition(gridPosition);
		return shard.getBlock(relativeBlockPosition);
	}

	public ShardIndex createIndex(final DatasetAttributes attributes) {

		return new ShardIndex(attributes.getBlocksPerShard(), getIndexLocation(), getIndexCodecAdapter());
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
