/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;

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
 * <li>{@link Codec}... : encode/decode routines</li>
 * </ol>
 *
 * @author Stephan Saalfeld
 */
//TODO : inline ShardParameters and delete interface
public class DatasetAttributes implements ShardParameters, Serializable {

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
	private final ArrayCodec<?> arrayCodec;
	private final BytesCodec[] byteCodecs;
	private final int[] shardSize;

	/**
	 * Constructs a DatasetAttributes instance with specified dimensions, block size, data type,
	 * and array of codecs.
	 *
	 * @param dimensions the dimensions of the dataset
	 * @param blockSize  the size of the blocks in the dataset
	 * @param dataType   the data type of the dataset
	 * @param codecs     the codecs used encode/decode the data
	 */
	public DatasetAttributes(
			final long[] dimensions,
			final int[] shardSize,
			final int[] blockSize,
			final DataType dataType,
			final Codec... codecs) {

		this.dimensions = dimensions;
		this.shardSize = shardSize;
		this.blockSize = blockSize;
		this.dataType = dataType;
		final Codec[] filteredCodecs = Arrays.stream(codecs).filter(it -> !(it instanceof RawCompression)).toArray(Codec[]::new);
		if (filteredCodecs.length == 0) {
			byteCodecs = new BytesCodec[]{};
			arrayCodec = new N5BlockCodec();
		} else if (filteredCodecs.length == 1 && filteredCodecs[0] instanceof Compression) {
			final BytesCodec compression = (BytesCodec)filteredCodecs[0];
			byteCodecs = compression instanceof RawCompression ? new BytesCodec[]{} : new BytesCodec[]{compression};
			arrayCodec = new N5BlockCodec();
		} else {
			if (!(filteredCodecs[0] instanceof ArrayCodec))
				throw new N5Exception("Expected first element of filteredCodecs to be ArrayCodec, but was: " + filteredCodecs[0].getClass());

			if (Arrays.stream(filteredCodecs).filter(c -> c instanceof ArrayCodec).count() > 1)
				throw new N5Exception("Multiple ArrayCodecs found. Only one is allowed.");

			arrayCodec = (ArrayCodec<?>)filteredCodecs[0];
			byteCodecs = Stream.of(filteredCodecs)
					.skip(1)
					.filter(c -> c instanceof BytesCodec)
					.toArray(BytesCodec[]::new);

		}
		//TODO Caleb: factory style for initialize
		arrayCodec.initialize(this, byteCodecs);
	}

	/**
	 * Constructs a DatasetAttributes instance with specified dimensions, block size, data type,
	 * and array of codecs.
	 *
	 * @param dimensions the dimensions of the dataset
	 * @param blockSize  the size of the blocks in the dataset
	 * @param dataType   the data type of the dataset
	 * @param codecs     the codecs used encode/decode the data
	 */
	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Codec... codecs) {
		this( dimensions, null, blockSize, dataType, codecs );
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
	public int[] getShardSize() {

		return shardSize;
	}

	@Override
	public int[] getBlockSize() {

		return blockSize;
	}

	/**
	 * Only used for deserialization for N5 backwards compatibility.
	 * {@link Compression} is no longer a special case. Prefer to reference {@link #getCodecs()}
	 * Will return {@link RawCompression} if no compression is otherwise provided, for legacy compatibility.
	 *
	 * @return compression Codec, if one was present, or else RawCompression
	 */
	@Deprecated
	public Compression getCompression() {

		return Arrays.stream(byteCodecs)
				.filter(it -> it instanceof Compression)
				.map(it -> (Compression)it)
				.findFirst()
				.orElse(new RawCompression());
	}

	public DataType getDataType() {

		return dataType;
	}

	/**
	 * Get the {@link ArrayCodec} for this dataset.
	 *
	 * @param <T>
	 * 		the returned codec is cast to {@code ArrayCodec<T>} for convenience
	 * 		(that is, the caller doesn't have to do the cast explicitly).
	 * @return the {@code ArrayCodec} for this dataset
	 */
	public <T> ArrayCodec<T> getArrayCodec() {

		return (ArrayCodec<T>) arrayCodec;
	}

	public BytesCodec[] getCodecs() {

		return byteCodecs;
	}

	/**
	 * Deprecated in favor of {@link DatasetAttributesAdapter} for serialization
	 *
	 * @return serilizable properties of {@link DatasetAttributes}
	 */
	@Deprecated
	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, getCompression());
		return map;
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

	public static class InvalidN5DatasetException extends N5Exception {

		public InvalidN5DatasetException(String dataset, String reason, Throwable cause) {

			this(String.format("Invalid dataset %s: %s", dataset, reason), cause);
		}

		public InvalidN5DatasetException(String message, Throwable cause) {

			super(message, cause);
		}
	}
	public static class DatasetAttributesAdapter implements JsonSerializer<DatasetAttributes>, JsonDeserializer<DatasetAttributes> {

		@Override public DatasetAttributes deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (json == null || !json.isJsonObject()) return null;
			final JsonObject obj = json.getAsJsonObject();
			final boolean validKeySet = obj.has(DIMENSIONS_KEY)
					&& obj.has(BLOCK_SIZE_KEY)
					&& obj.has(DATA_TYPE_KEY)
					&& (obj.has(CODEC_KEY) || obj.has(COMPRESSION_KEY) || obj.has(compressionTypeKey));

			if (!validKeySet)
				return null;

			final long[] dimensions = context.deserialize(obj.get(DIMENSIONS_KEY), long[].class);
			final int[] blockSize = context.deserialize(obj.get(BLOCK_SIZE_KEY), int[].class);

			int[] shardSize = null;
			if (obj.has(SHARD_SIZE_KEY))
				shardSize = context.deserialize(obj.get(SHARD_SIZE_KEY), int[].class);

			final DataType dataType = context.deserialize(obj.get(DATA_TYPE_KEY), DataType.class);


			final Codec[] codecs;
			if (obj.has(CODEC_KEY)) {
				codecs = context.deserialize(obj.get(CODEC_KEY), Codec[].class);
			} else if (obj.has(COMPRESSION_KEY)) {
				final Compression compression = CompressionAdapter.getJsonAdapter().deserialize(obj.get(COMPRESSION_KEY), Compression.class, context);
				final N5BlockCodec<?> n5BlockCodec = new N5BlockCodec<>();
				codecs = new Codec[]{compression, n5BlockCodec};
			} else if (obj.has(compressionTypeKey)) {
				final Compression compression = getCompressionVersion0(obj.get(compressionTypeKey).getAsString());
				final N5BlockCodec<?> n5BlockCodec = new N5BlockCodec<>();
				codecs = new Codec[]{compression, n5BlockCodec};
			} else {
				return null;
			}
			return new DatasetAttributes(dimensions, shardSize, blockSize, dataType, codecs);
		}

		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));

			//TODO Caleb: Type Hierarchy Adapter for extensions?
			final int[] shardSize = src.getShardSize();
			if (shardSize != null) {
				obj.add(SHARD_SIZE_KEY, context.serialize(shardSize));
			}

			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));
			obj.add(CODEC_KEY, context.serialize(src.concatenateCodecs()));

			return obj;
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
	}
}
