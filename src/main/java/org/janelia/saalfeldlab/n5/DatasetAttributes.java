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
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.BlockAsShardCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.util.Position;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;

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

	// number of samples per shard per dimension
	private final int[] shardSize;

	private final DataType dataType;

	private final ShardingCodec shardingCodec;
	private final BlockCodecInfo blockCodecInfo;
	private final DataCodecInfo[] dataCodecInfos;

	private final BlockCodec<?> blockCodec;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] shardSize,
			final int[] blockSize,
			final DataType dataType,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo... dataCodecInfos) {

		validateBlockShardSizes(dimensions, shardSize, blockSize);

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.shardSize = shardSize;
		this.dataType = dataType;

		this.blockCodecInfo = blockCodecInfo == null ? defaultBlockCodecInfo() : blockCodecInfo;
		this.dataCodecInfos = Arrays.stream(dataCodecInfos).filter(it -> !(it instanceof RawCompression)).toArray(DataCodecInfo[]::new);
		if (this.blockCodecInfo instanceof ShardingCodec)
			shardingCodec = (ShardingCodec)this.blockCodecInfo;
		else
			shardingCodec = new BlockAsShardCodec(this.blockCodecInfo);
		blockCodec = this.shardingCodec.create(this, dataCodecInfos);

	}

	private void validateBlockShardSizes(long[] dimensions, int[] shardSize, int[] blockSize) {

		final int nd = dimensions.length;

		if (blockSize.length != nd)
			throw new IllegalArgumentException(String.format("Number of block dimensions (%d) must equal number of dimensions (%d).",
					blockSize.length, nd));

		if (shardSize.length != nd)
			throw new IllegalArgumentException(String.format("Number of shard dimensions (%d) must equal number of dimensions (%d).",
					shardSize.length, nd));

		for (int i = 0; i < blockSize.length; i++) {

			if (blockSize[i] <= 0)
				throw new IllegalArgumentException(String.format("Block size in dimension %d (%d) is <= 0",
						i, blockSize[i]));

			if (shardSize[i] < blockSize[i])
				throw new IllegalArgumentException(String.format("Shard size in dimension %d (%d) is larger than the block size (%d)",
						i, shardSize[i], blockSize[i]));
			else if (shardSize[i] % blockSize[i] != 0)
				throw new IllegalArgumentException(String.format("Shard size in dimension %d (%d) not a multiple of the block size (%d)",
						i, shardSize[i], blockSize[i]));
		}
	}

	protected BlockCodecInfo defaultArrayCodec() {

		return new N5BlockCodecInfo();
	}

	/**
	 * Constructs a DatasetAttributes instance with specified dimensions, block size, data type,
	 * and single compressor with default codec.
	 *
	 * @param dimensions  the dimensions of the dataset
	 * @param blockSize   the size of the blocks in the dataset
	 * @param dataType    the data type of the dataset
	 * @param compression the codecs used encode/decode the data
	 */
	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final DataCodecInfo compression) {

		this(dimensions, blockSize, blockSize, dataType, null, compression);
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

		this(dimensions, blockSize, blockSize, dataType, null);
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

	public int[] getShardSize() {

		return shardSize;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	/**
	 * Returns the number of blocks per dimension for each shard.
	 *
	 * @return the blocks per shard
	 */
	public int[] getBlocksPerShard() {

		final int[] shardSize = getShardSize();
		final int nd = getNumDimensions();
		final int[] blocksPerShard = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			blocksPerShard[i] = shardSize[i] / blockSize[i];

		return blocksPerShard;
	}

	/**
	 * Returns the number of blocks per dimension for this dataset.
	 *
	 * @return blocks per dataset
	 */
	public long[] blocksPerDataset() {

		return IntStream.range(0, getNumDimensions())
				.mapToLong(i -> (long)Math.ceil((double)getDimensions()[i] / getBlockSize()[i]))
				.toArray();
	}

	/**
	 * Returns the number of shards per dimension for this dataset.
	 *
	 * @return shards per dataset
	 */
	public long[] shardsPerDataset() {

		return IntStream.range(0, getNumDimensions())
				.mapToLong(i -> (long)Math.ceil((double)getDimensions()[i] / getShardSize()[i]))
				.toArray();
	}

	/**
	 * Returns the total number of blocks in each shard.
	 *
	 * @return number of blocks in a shard
	 */
	public long getNumBlocksPerShard() {

		return Arrays.stream(getBlocksPerShard()).reduce(1, (x, y) -> x * y);
	}

	public long[] getKeyPositionForBlock(final long... blockGridPosition) {
		return getBlockCodecInfo().getKeyPositionForBlock(this, blockGridPosition);
	}

	/**
	 * Given a block's position relative to the dataset, returns the position of
	 * the shard containing that block.
	 *
	 * @param blockGridPosition position of a block relative to the dataset
	 * @return the position of the containing shard in the shard grid
	 */
	public long[] getShardPositionForBlock(final long... blockGridPosition) {

		final int[] blocksPerShard = getBlocksPerShard();
		final long[] shardGridPosition = new long[blockGridPosition.length];
		for (int i = 0; i < shardGridPosition.length; i++) {
			shardGridPosition[i] = (int)Math.floor((double)blockGridPosition[i] / blocksPerShard[i]);
		}

		return shardGridPosition;
	}

	/**
	 * Given a {@code datasetRelativeBlockPosition} returns the position
	 * relative the the shard at position {@code shardPosition}.
	 *
	 * @param shardPosition                position of the shard
	 * @param datasetRelativeBlockPosition position of the block relative to the dataset
	 * @return position of the block relative to the shard
	 */
	public int[] getShardRelativeBlockPosition(final long[] shardPosition, final long[] datasetRelativeBlockPosition) {

		final long[] shardPos = getShardPositionForBlock(datasetRelativeBlockPosition);
		if (!Arrays.equals(shardPosition, shardPos))
			return null;

		final int[] shardSize = getBlocksPerShard();
		final int[] shardRelativeBlockPosition = new int[shardSize.length];
		for (int i = 0; i < shardSize.length; i++) {
			shardRelativeBlockPosition[i] = (int)(datasetRelativeBlockPosition[i] % shardSize[i]);
		}
		return shardRelativeBlockPosition;
	}

	/**
	 * Given a {@code shardRelativeBlockPosition} relative to the shard at
	 * position {@code shardPosition}, returns the block' position relative the
	 * dataset.
	 *
	 * @param shardPosition              position of the shard
	 * @param shardRelativeBlockPosition position of the block relative to the shard
	 * @return position of the block relative to the dataset
	 */
	public long[] getBlockPositionFromShardPosition(final long[] shardPosition, final int[] shardRelativeBlockPosition) {

		final int[] shardBlockSize = getBlocksPerShard();
		final long[] datasetRelativeBlockPosition = new long[getNumDimensions()];
		for (int i = 0; i < getNumDimensions(); i++) {
			datasetRelativeBlockPosition[i] = (shardPosition[i] * shardBlockSize[i]) + (shardRelativeBlockPosition[i]);
		}

		return datasetRelativeBlockPosition;
	}

	/**
	 * Returns the number of shards per dimension for the dataset.
	 *
	 * @return the size of the shard grid of a dataset
	 */
	public int[] getShardBlockGridSize() {

		final int nd = getNumDimensions();
		final int[] shardBlockGridSize = new int[nd];
		final int[] blockSize = getBlockSize();
		for (int i = 0; i < nd; i++)
			shardBlockGridSize[i] = (int)(Math.ceil((double)getDimensions()[i] / blockSize[i]));

		return shardBlockGridSize;
	}

	public Map<Position, List<long[]>> groupBlockPositions(final List<long[]> blockPositions) {

		final TreeMap<Position, List<long[]>> map = new TreeMap<>();
		for (final long[] blockPos : blockPositions) {
			Position shardPos = Position.wrap(getShardPositionForBlock(blockPos));
			if (!map.containsKey(shardPos)) {
				map.put(shardPos, new ArrayList<>());
			}
			map.get(shardPos).add(blockPos);
		}

		return map;
	}

	public <T> Map<Position, List<DataBlock<T>>> groupBlocks(final List<DataBlock<T>> blocks) {

		// figure out how to re-use groupBlockPositions here?
		final TreeMap<Position, List<DataBlock<T>>> map = new TreeMap<>();
		for (final DataBlock<T> block : blocks) {
			Position shardPos = Position.wrap(getShardPositionForBlock(block.getGridPosition()));
			if (!map.containsKey(shardPos)) {
				map.put(shardPos, new ArrayList<>());
			}
			map.get(shardPos).add(block);
		}

		return map;
	}

	public boolean isSharded() {

		return getBlockCodecInfo() instanceof ShardingCodec;
	}

	/**
	 * @return the ShardingCodec
	 */
	public ShardingCodec getShardingCodec() {

		return shardingCodec;
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
	 * Get the {@link BlockCodecInfo} for this dataset.
	 *
	 * @return the {@code BlockCodecInfo} for this dataset
	 */
	public BlockCodecInfo getBlockCodecInfo() {

		return blockCodecInfo;
	}

	public DataCodecInfo[] getDataCodecInfos() {

		return dataCodecInfos;
	}

	@SuppressWarnings("unchecked")
	public <T> BlockCodec<T> getBlockCodec() {

		return (BlockCodec<T>) blockCodec;
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
			final int[] shardSize = blockSize;

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

			return new DatasetAttributes(dimensions, shardSize, blockSize, dataType, blockCodecInfo, dataCodecs);
		}

		//FIXME
		// this implements multi-codec serialization for N5. We probably don't want this now
		@Override public JsonElement serialize(DatasetAttributes src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add(DIMENSIONS_KEY, context.serialize(src.dimensions));
			obj.add(BLOCK_SIZE_KEY, context.serialize(src.blockSize));
			obj.add(DATA_TYPE_KEY, context.serialize(src.dataType));

			final DataCodecInfo[] codecs = src.getDataCodecInfos();
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
