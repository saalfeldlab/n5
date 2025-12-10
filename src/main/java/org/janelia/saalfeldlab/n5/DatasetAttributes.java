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
package org.janelia.saalfeldlab.n5;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.DatasetAccess;
import org.janelia.saalfeldlab.n5.shard.DefaultDatasetAccess;
import org.janelia.saalfeldlab.n5.shard.ShardCodecInfo;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;


/**
 * Mandatory dataset attributes:
 *
 * <ol>
 * <li>long[] : dimensions</li>
 * <li>int[] : blockSize</li>
 * <li>{@link DataType} : dataType</li>
 * <li>{@link CodecInfo}... : encode/decode routines</li>
 * </ol>
 *
 * @author Stephan Saalfeld
 */
public class DatasetAttributes implements Serializable {

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

	// number of samples per block per dimension
	private final int[] blockSize;

	// TODO add a getter?
	// the shard size
	private final int[] outerBlockSize;

	private final DataType dataType;

	private final JsonElement defaultValue;

	private final BlockCodecInfo blockCodecInfo;
	private final DataCodecInfo[] dataCodecInfos;
	private final DatasetCodecInfo[] datasetCodecInfos;

	private transient final DatasetAccess<?> access;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] outerBlockSize,
			final DataType dataType,
			final JsonElement defaultValue,
			final BlockCodecInfo blockCodecInfo,
			final DatasetCodecInfo[] datasetCodecInfos,
			final DataCodecInfo... dataCodecInfos) {

		this.dimensions = dimensions;
		this.dataType = dataType;
		this.outerBlockSize = outerBlockSize;
		this.defaultValue = defaultValue == null ? JsonNull.INSTANCE : defaultValue;

		this.blockCodecInfo = blockCodecInfo == null ? defaultBlockCodecInfo() : blockCodecInfo;
		this.datasetCodecInfos = datasetCodecInfos;

		if (dataCodecInfos == null)
			this.dataCodecInfos = new DataCodecInfo[0];
		else
			this.dataCodecInfos = Arrays.stream(dataCodecInfos)
					.filter(it -> it != null && !(it instanceof RawCompression))
					.toArray(DataCodecInfo[]::new);

		access = createDatasetAccess();
		blockSize = access.getGrid().getBlockSize(0);
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] outerBlockSize,
			final DataType dataType,
			final BlockCodecInfo blockCodecInfo,
			final DatasetCodecInfo[] datasetCodecInfos,
			final DataCodecInfo... dataCodecInfos) {

		this(dimensions, outerBlockSize, dataType, JsonNull.INSTANCE,
				blockCodecInfo, datasetCodecInfos, dataCodecInfos);
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] outerBlockSize,
			final DataType dataType,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		this(dimensions, outerBlockSize, dataType, blockCodecInfo, null, dataCodecInfos);
	}

	/**
	 * Constructs a DatasetAttributes instance with specified dimensions, block size, data type,
	 * and single compressor with default codec.
	 *
	 * @param dimensions  the dimensions of the dataset
	 * @param blockSize   the size of the blocks in the dataset
	 * @param dataType    the data type of the dataset
	 * @param dataCodecInfos the codecs used encode/decode the data
	 */
	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final DataCodecInfo... dataCodecInfos) {

		this(dimensions, blockSize, dataType, null, dataCodecInfos);
	}

	/**
	 * Constructs a DatasetAttributes instance with specified dimensions, block size, data type, and default codecs
	 *
	 * @param dimensions the dimensions of the dataset
	 * @param blockSize  the size of the blocks in the dataset
	 * @param dataType   the data type of the dataset
	 */
	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType) {

		this(dimensions, blockSize, dataType, new DataCodecInfo[0]);
	}

	protected DatasetAccess<?> createDatasetAccess() {

		final int m = nestingDepth(blockCodecInfo);

		// There are m codecs: 1 DataBlock codecs, and m-1 shard codecs.
		// The inner-most codec (the DataBlock codec) is at index 0.
		final int[][] blockSizes = new int[m][];

		// NestedGrid validates block sizes, so instantiate it before creating the blockCodecs  
		// blockCodecInfo.create below could fail unexpecedly with invalid
		// blockSizes so validate first
		blockSizes[m - 1] = outerBlockSize;
		BlockCodecInfo tmpInfo = blockCodecInfo;
		for (int l = m - 1; l > 0; --l) {
			final ShardCodecInfo info = (ShardCodecInfo)tmpInfo;
			blockSizes[l - 1] = info.getInnerBlockSize();
			tmpInfo = info.getInnerBlockCodecInfo();
		}

		BlockCodecInfo currentBlockCodecInfo = blockCodecInfo;
		DataCodecInfo[] currentDataCodecInfos = dataCodecInfos;

		final NestedGrid grid = new NestedGrid(blockSizes);
		final BlockCodec<?>[] blockCodecs = new BlockCodec[m];
		for (int l = m - 1; l >= 0; --l) {
			blockCodecs[l] = currentBlockCodecInfo.create(dataType, blockSizes[l], currentDataCodecInfos);
			if (l > 0) {
				final ShardCodecInfo info = (ShardCodecInfo)currentBlockCodecInfo;
				currentBlockCodecInfo = info.getInnerBlockCodecInfo();
				currentDataCodecInfos = info.getInnerDataCodecInfos();
			}
		}

		final DatasetCodec[] datasetCodecs;
		if (datasetCodecInfos != null) {
			datasetCodecs = new DatasetCodec[datasetCodecInfos.length];
			for (int i = 0; i < datasetCodecInfos.length; i++)
				datasetCodecs[i] = datasetCodecInfos[i].create(this);

		} else
			datasetCodecs = new DatasetCodec[0];

		return new DefaultDatasetAccess<>(grid, blockCodecs, datasetCodecs);
	}

	private static int nestingDepth(BlockCodecInfo info) {

		if (info instanceof ShardCodecInfo) {
			return 1 + nestingDepth(((ShardCodecInfo)info).getInnerBlockCodecInfo());
		} else {
			return 1;
		}
	}

	protected BlockCodecInfo defaultBlockCodecInfo() {

		return new N5BlockCodecInfo();
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

	public JsonElement getDefaultValue() {

		return defaultValue;
	}

	public boolean isSharded() {

		return blockCodecInfo instanceof ShardCodecInfo;
	}

	/**
	 * Only used for deserialization for N5 backwards compatibility.
	 * {@link Compression} is no longer a special case. Prefer to reference {@link #getDataCodecInfos()}
	 * Will return {@link RawCompression} if no compression is otherwise provided, for legacy compatibility.
	 * <p>
	 * Deprecated in favor of {@link #getDataCodecInfos()}.
	 *
	 * @return compression CodecInfo, if one was present, or else RawCompression
	 */
	@Deprecated
	public Compression getCompression() {

		return Arrays.stream(dataCodecInfos)
				.filter(it -> it instanceof Compression)
				.map(it -> (Compression)it)
				.findFirst()
				.orElse(new RawCompression());
	}

	public DataType getDataType() {

		return dataType;
	}

	/**
	 * Get the {@link DatasetAccess} for this dataset.
	 *
	 * @return the {@code DatasetAccess} for this dataset
	 */
	<T> DatasetAccess<T> getDatasetAccess() {

		return (DatasetAccess<T>)access;
	}

	/**
	 * Returns the {@code NestedGrid} for this dataset, from which block and
	 * shard sizes are accessible.
	 *
	 * @return the NestedGrid
	 */
	public NestedGrid getNestedBlockGrid() {

		return getDatasetAccess().getGrid();
	}


	public BlockCodecInfo getBlockCodecInfo() {

		return blockCodecInfo;
	}

	public DataCodecInfo[] getDataCodecInfos() {

		return dataCodecInfos;
	}

	public DatasetCodecInfo[] getDatasetCodecInfos() {

		return datasetCodecInfos;
	}

	public String relativeBlockPath(long... position) {

		return Arrays.stream(position).mapToObj(Long::toString).collect(Collectors.joining("/"));
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, getCompression());
		return map;
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

			if (json == null || !json.isJsonObject())
				return null;
			final JsonObject obj = json.getAsJsonObject();
			final boolean validKeySet = obj.has(DIMENSIONS_KEY)
					&& obj.has(BLOCK_SIZE_KEY)
					&& obj.has(DATA_TYPE_KEY)
					&& (obj.has(CODEC_KEY) || obj.has(COMPRESSION_KEY) || obj.has(compressionTypeKey));

			if (!validKeySet)
				return null;

			final long[] dimensions = context.deserialize(obj.get(DIMENSIONS_KEY), long[].class);
			final int[] blockSize = context.deserialize(obj.get(BLOCK_SIZE_KEY), int[].class);

			final DataType dataType = context.deserialize(obj.get(DATA_TYPE_KEY), DataType.class);

			final BlockCodecInfo blockCodecInfo;
			final DataCodecInfo[] dataCodecs;
			if (obj.has(CODEC_KEY)) {
				final CodecInfo[] codecs = context.deserialize(obj.get(CODEC_KEY), CodecInfo[].class);
				blockCodecInfo = (BlockCodecInfo)codecs[0];
				dataCodecs = new DataCodecInfo[codecs.length - 1];
				for (int i = 1; i < codecs.length; i++) {
					dataCodecs[i - 1] = (DataCodecInfo)codecs[i];
				}
			} else if (obj.has(COMPRESSION_KEY)) {
				final Compression compression = CompressionAdapter.getJsonAdapter().deserialize(obj.get(COMPRESSION_KEY), Compression.class, context);
				dataCodecs = new DataCodecInfo[]{compression};
				blockCodecInfo = new N5BlockCodecInfo();
			} else if (obj.has(compressionTypeKey)) {
				final Compression compression = getCompressionVersion0(obj.get(compressionTypeKey).getAsString());
				dataCodecs = new DataCodecInfo[]{compression};
				blockCodecInfo = new N5BlockCodecInfo();
			} else {
				return null;
			}

			return new DatasetAttributes(dimensions, blockSize, dataType, blockCodecInfo, dataCodecs);
		}

		//FIXME
		// this implements multi-codec serialization for N5. We probably don't want this now
		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));
			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));

			final DataCodecInfo[] codecs = src.dataCodecInfos;
			// length > 1 is actually invalid, but this is checked on construction
			if (codecs.length == 0)
				obj.add(COMPRESSION_KEY, context.serialize(new RawCompression()));
			else
				obj.add(COMPRESSION_KEY, context.serialize(codecs[0]));

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
