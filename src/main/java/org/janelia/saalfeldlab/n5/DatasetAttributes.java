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
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.ComposedCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;

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
			final Codec[] codecs ) {

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.dataType = dataType;
		this.codecs = codecs;
		this.compression = compression;
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

	public Codec collectCodecs() {

		if (codecs == null || codecs.length == 0)
			return compression;
		else if (codecs.length == 1)
			return new ComposedCodec(codecs[0], compression);
		else {
			final Codec[] codecsAndCompresor = new Codec[codecs.length + 1];
			for (int i = 0; i < codecs.length; i++)
				codecsAndCompresor[i] = codecs[i];

			codecsAndCompresor[codecs.length] = compression;
			return new ComposedCodec(codecsAndCompresor);
		}
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
			switch (compressionVersion0Name) {
			case "raw":
				compression = new RawCompression();
				break;
			case "gzip":
				compression = new GzipCompression();
				break;
			case "bzip2":
				compression = new Bzip2Compression();
				break;
			case "lz4":
				compression = new Lz4Compression();
				break;
			case "xz":
				compression = new XzCompression();
				break;
			}
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression, codecs);
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
			final Compression compression = context.deserialize(obj.get(COMPRESSION_KEY), Compression.class);
			final Codec[] codecs;
			if (obj.has(CODEC_KEY)) {
				codecs = context.deserialize(obj.get(CODEC_KEY), Codec[].class);
			} else codecs = new Codec[0];

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
			return new DatasetAttributes(dimensions, blockSize, dataType, compression);
		}

		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));
			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));
			obj.add(COMPRESSION_KEY, CompressionAdapter.getJsonAdapter().serialize(src.compression, src.compression.getClass(), context));

			//TODO Caleb: Per the zarr v3 spec, codecs is necessary and cannot be an empty list, since it always needs at least
			//	one array -> bytes codec. Even in the case of no compressor, there should always be at least the
			//	`bytes` codec it seems. Consider how we want to handle this in N5
			obj.add(CODEC_KEY, context.serialize(src.codecs));

			return obj;
		}
	}
}
