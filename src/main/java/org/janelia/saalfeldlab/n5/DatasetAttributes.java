package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;

import javax.xml.crypto.Data;

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
public class DatasetAttributes implements Serializable {

	private static final long serialVersionUID = -4521467080388947553L;

	public static final String DIMENSIONS_KEY = "dimensions";
	public static final String BLOCK_SIZE_KEY = "blockSize";
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
	private final Codec[] codecs;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.dataType = dataType;
		this.compression = compression;
		if (codecs == null && !(compression instanceof RawCompression)) {
			this.codecs = new Codec[]{new BytesCodec(), compression};
		} else if (codecs == null) {
			this.codecs = new Codec[]{new BytesCodec()};
		} else {
			this.codecs = codecs;
		}
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) {

		this(dimensions, blockSize, dataType, compression, null);
	}

	public long[] getDimensions() {

		return dimensions;
	}

	public int getNumDimensions() {

		return dimensions.length;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public Compression getCompression() {

		return compression;
	}

	public DataType getDataType() {

		return dataType;
	}

	public Codec[] getCodecs() {

		return codecs;
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, compression);
		map.put(CODEC_KEY, codecs); // TODO : consider not adding to map when null
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

	private static DatasetAttributesAdapter adapter = null;
	public static DatasetAttributesAdapter getJsonAdapter() {
		if (adapter == null) {
			adapter = new DatasetAttributesAdapter();
		}
		return adapter;
	}

	public static class DatasetAttributesAdapter implements JsonSerializer<DatasetAttributes>, JsonDeserializer<DatasetAttributes> {

		@Override public DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			final JsonObject obj = json.getAsJsonObject();
			if (!obj.has(DIMENSIONS_KEY) || !obj.has(BLOCK_SIZE_KEY) || !obj.has(DATA_TYPE_KEY) || !obj.has(COMPRESSION_KEY))
				return null;

			final long[] dimensions = context.deserialize(obj.get(DIMENSIONS_KEY), long[].class);
			final int[] blockSize = context.deserialize(obj.get(BLOCK_SIZE_KEY), int[].class);
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

			for (Codec codec : codecs) {
				if (codec instanceof ShardingCodec) {
					ShardingCodec shardingCodec = (ShardingCodec)codec;
					return new ShardedDatasetAttributes(
							dimensions,
							shardingCodec.getBlockSize(),
							blockSize,
							shardingCodec.getIndexLocation(),
							dataType,
							compression,
							codecs
					);
				}
			}
			return new DatasetAttributes(dimensions, blockSize, dataType, compression, codecs);
		}

		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));
			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));
			obj.add(COMPRESSION_KEY, CompressionAdapter.getJsonAdapter().serialize(src.compression, src.compression.getClass(), context));
			obj.add(CODEC_KEY, context.serialize(src.codecs));

			return obj;
		}
	}
}
