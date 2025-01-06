package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Mandatory dataset attributes:
 *
 * <ol>
 * <li>long[] : dimensions</li>
 * <li>int[] : blockSize</li>
 * <li>{@link DataType} : dataType</li>
 * <li>{@link Compression} : compression</li>
 * </ol>
 *
 * Optional dataset attributes:
 * <ol>
 * <li>{@link Codec}[] : codecs</li>
 * </ol>
 *
 * @author Stephan Saalfeld
 *
 */
public class DatasetAttributes implements BlockParameters, Serializable {

	private static final long serialVersionUID = -4521467080388947553L;

	public static final String DIMENSIONS_KEY = "dimensions";
	public static final String BLOCK_SIZE_KEY = "blockSize";
	public static final String SHARD_SIZE_KEY = "shardSize";
	public static final String DATA_TYPE_KEY = "dataType";
	public static final String COMPRESSION_KEY = "compression";
	public static final String CODEC_KEY = "codecs";

	public static final String[] N5_DATASET_ATTRIBUTES = new String[]{
			DIMENSIONS_KEY, BLOCK_SIZE_KEY, DATA_TYPE_KEY, COMPRESSION_KEY, CODEC_KEY
	};

	/* version 0 */
	protected static final String compressionTypeKey = "compressionType";

	private final long[] dimensions;
	private final int[] blockSize;
	private final DataType dataType;
	private final Compression compression;
	private final ArrayCodec arrayCodec;
	private final BytesCodec[] byteCodecs;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.dataType = dataType;
		if (codecs == null && !(compression instanceof RawCompression)) {
			byteCodecs = new BytesCodec[]{compression};
			arrayCodec = new N5BlockCodec();
		} else if (codecs == null || codecs.length == 0) {
			byteCodecs = new BytesCodec[]{};
			arrayCodec = new N5BlockCodec();
		} else {
			if (!(codecs[0] instanceof ArrayCodec))
				throw new N5Exception("Expected first element of codecs to be ArrayCodec, but was: " + codecs[0]);

			arrayCodec = (ArrayCodec)codecs[0];
			byteCodecs = new BytesCodec[codecs.length - 1];
			for (int i = 0; i < byteCodecs.length; i++)
				byteCodecs[i] = (BytesCodec)codecs[i + 1];
		}

		//TODO Caleb: Do we want to do this?
		this.compression = Arrays.stream(byteCodecs)
				.filter(codec -> codec instanceof Compression)
				.map(codec -> (Compression)codec)
				.findFirst()
				.orElse(compression == null ? new RawCompression() : compression);

	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Codec[] codecs) {
		this(dimensions, blockSize, dataType, null, codecs);
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) {

		this(dimensions, blockSize, dataType, compression, null);
	}

	@Override
	public long[] getDimensions() {

		return dimensions;
	}

	@Override
	public int getNumDimensions() {

		return dimensions.length;
	}

	@Override
	public int[] getBlockSize() {

		return blockSize;
	}

	public Compression getCompression() {

		return compression;
	}

	public DataType getDataType() {

		return dataType;
	}

	public ArrayCodec getArrayCodec() {

		return arrayCodec;
	}

	public BytesCodec[] getCodecs() {

		return byteCodecs;
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, compression);
		map.put(CODEC_KEY, concatenateCodecs()); // TODO : consider not adding to map when null
		return map;
	}

	static DatasetAttributes from(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name) {

		return from(dimensions, dataType, blockSize, compression, compressionVersion0Name, null);
	}

	static DatasetAttributes from(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name,
			Codec[] codecs) {

		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		/* version 0 */
		if (compression == null) {
			compression = getCompressionVersion0(compressionVersion0Name);
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression, codecs);
	}

	private static Compression getCompressionVersion0(final String compressionVersion0Name) {

		switch (compressionVersion0Name) {
		case "raw":
			return new RawCompression();
		case "gzip":
			return new GzipCompression();
		case "bzip2":
			return new Bzip2Compression();
		case "lz4":
			return new Lz4Compression();
		case "xz":
			return new XzCompression();
		}
		return null;
	}

	protected Codec[] concatenateCodecs() {

		final Codec[] allCodecs = new Codec[byteCodecs.length + 1];
		allCodecs[0] = arrayCodec;
		for (int i = 0; i < byteCodecs.length; i++)
			allCodecs[i + 1] = byteCodecs[i];

		return allCodecs;
	}

	private static DatasetAttributesAdapter adapter = null;
	public static DatasetAttributesAdapter getJsonAdapter() {
		if (adapter == null) {
			adapter = new DatasetAttributesAdapter();
		}
		return adapter;
	}

	public static class DatasetAttributesAdapter implements JsonSerializer<DatasetAttributes>, JsonDeserializer<DatasetAttributes> {

		@Override public DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (json == null || !json.isJsonObject()) return null;
			final JsonObject obj = json.getAsJsonObject();
			if (!obj.has(DIMENSIONS_KEY) || !obj.has(BLOCK_SIZE_KEY) || !obj.has(DATA_TYPE_KEY) || !obj.has(COMPRESSION_KEY))
				return null;

			final long[] dimensions = context.deserialize(obj.get(DIMENSIONS_KEY), long[].class);
			final int[] blockSize = context.deserialize(obj.get(BLOCK_SIZE_KEY), int[].class);

			int[] shardSize = null;
			if (obj.has(SHARD_SIZE_KEY)) {
				shardSize = context.deserialize(obj.get(SHARD_SIZE_KEY), int[].class);
			}

			final DataType dataType = context.deserialize(obj.get(DATA_TYPE_KEY), DataType.class);

			Compression compression = null;
			if (obj.has(COMPRESSION_KEY)) {
				compression = CompressionAdapter.getJsonAdapter().deserialize(obj.get(COMPRESSION_KEY), Compression.class, context);
			} else if (obj.has(compressionTypeKey)) {
				compression = DatasetAttributes.getCompressionVersion0(obj.get(compressionTypeKey).getAsString());
			}
			if (compression == null)
				return null;

			final Codec[] codecs;
			if (obj.has(CODEC_KEY)) {
				codecs = context.deserialize(obj.get(CODEC_KEY), Codec[].class);
			} else codecs = null;

			if (codecs != null && codecs.length == 1 && codecs[0] instanceof ShardingCodec) {
				final ShardingCodec shardingCodec = (ShardingCodec)codecs[0];
				return new ShardedDatasetAttributes(
						dimensions,
						shardSize,
						blockSize,
						dataType,
						shardingCodec
				);
			}
			return new DatasetAttributes(dimensions, blockSize, dataType, compression, codecs);
		}

		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));

			if (src instanceof ShardedDatasetAttributes) {
				obj.add(SHARD_SIZE_KEY, context.serialize(((ShardedDatasetAttributes)src).getShardSize()));
			}

			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));
			obj.add(COMPRESSION_KEY, CompressionAdapter.getJsonAdapter().serialize(src.compression, src.compression.getClass(), context));
			obj.add(CODEC_KEY, context.serialize(src.concatenateCodecs()));

			return obj;
		}
	}
}
