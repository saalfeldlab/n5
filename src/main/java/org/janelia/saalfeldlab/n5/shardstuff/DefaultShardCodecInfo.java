package org.janelia.saalfeldlab.n5.shardstuff;

import java.lang.reflect.Type;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Default (and probably only) implementation of {@link ShardCodecInfo}.
 */
// TODO rename?
@NameConfig.Name(value = "ShardingCodec")
public class DefaultShardCodecInfo implements ShardCodecInfo {

	@Override
	public String getType() {
		return "ShardingCodec";
	}

	private final int[] innerBlockSize;
	private final BlockCodecInfo innerBlockCodecInfo;
	private final DataCodecInfo[] innerDataCodecInfos;
	private final BlockCodecInfo indexBlockCodecInfo;
	private final DataCodecInfo[] indexDataCodecInfos;
	private final IndexLocation indexLocation;

	DefaultShardCodecInfo() {
		// for serialization
		this(null, null, null, null, null, null);
	}

	public DefaultShardCodecInfo(
			final int[] innerBlockSize,
			final BlockCodecInfo innerBlockCodecInfo,
			final DataCodecInfo[] innerDataCodecInfos,
			final BlockCodecInfo indexBlockCodecInfo,
			final DataCodecInfo[] indexDataCodecInfos,
			final IndexLocation indexLocation) {
		this.innerBlockSize = innerBlockSize;
		this.innerBlockCodecInfo = innerBlockCodecInfo;
		this.innerDataCodecInfos = innerDataCodecInfos;
		this.indexBlockCodecInfo = indexBlockCodecInfo;
		this.indexDataCodecInfos = indexDataCodecInfos;
		this.indexLocation = indexLocation;
	}

	@Override
	public int[] getInnerBlockSize() {
		return innerBlockSize;
	}

	@Override
	public BlockCodecInfo getInnerBlockCodecInfo() {
		return innerBlockCodecInfo;
	}

	@Override
	public DataCodecInfo[] getInnerDataCodecInfos() {
		return innerDataCodecInfos;
	}

	@Override
	public BlockCodecInfo getIndexBlockCodecInfo() {
		return indexBlockCodecInfo;
	}

	@Override
	public DataCodecInfo[] getIndexDataCodecInfos() {
		return indexDataCodecInfos;
	}

	@Override
	public IndexLocation getIndexLocation() {
		return indexLocation;
	}

	@Override
	public RawShardCodec create(final int[] blockSize, final DataCodecInfo... codecs) {

		// Number of elements (DataBlocks, nested shards) in each dimension per shard.
		final int[] size = new int[blockSize.length];
		// blockSize argument is number of pixels in the shard
		// innerBlockSize is number of pixels in each shard element (nested shard or DataBlock)
		Arrays.setAll(size, d -> blockSize[d] / innerBlockSize[d]);

		final BlockCodec<long[]> indexCodec = indexBlockCodecInfo.create(
				DataType.UINT64,
				ShardIndex.blockSizeFromIndexSize(size),
				indexDataCodecInfos);

		return new RawShardCodec(size, indexLocation, indexCodec);
	}

	public static DefaultShardCodecInfoAdapter adapter = new DefaultShardCodecInfoAdapter();

	public static class DefaultShardCodecInfoAdapter implements JsonDeserializer<DefaultShardCodecInfo>, JsonSerializer<DefaultShardCodecInfo> {

		@Override
		public DefaultShardCodecInfo deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (!json.isJsonObject())
				return null;

			JsonObject obj = json.getAsJsonObject();

			int[] innerBlockSize = context.deserialize(obj.get("innerBlockSize"), int[].class);
			BlockCodecInfo innerBlockCodecInfo = context.deserialize(obj.get("innerBlockCodecInfo"), BlockCodecInfo[].class);
			DataCodecInfo[] innerDataCodecInfos = context.deserialize(obj.get("innerDataCodecInfos"), DataCodecInfo[].class);
			BlockCodecInfo indexBlockCodecInfo = context.deserialize(obj.get("indexBlockCodecInfo"), BlockCodecInfo[].class);
			DataCodecInfo[] indexDataCodecInfos = context.deserialize(obj.get("indexDataCodecInfos"), DataCodecInfo[].class);
			IndexLocation indexLocation = IndexLocation.valueOf(obj.get("indexLocation").getAsString());

			return new DefaultShardCodecInfo(innerBlockSize, innerBlockCodecInfo, innerDataCodecInfos, indexBlockCodecInfo, indexDataCodecInfos, indexLocation);
		}

		@Override
		public JsonElement serialize(DefaultShardCodecInfo src, Type typeOfSrc, JsonSerializationContext context) {

			final JsonObject obj = new JsonObject();
			obj.add("innerBlockSize", context.serialize(src.innerBlockSize));
			obj.add("innerBlockCodecInfo", context.serialize(src.innerBlockCodecInfo));
			obj.add("innerDataCodecInfos", context.serialize(src.innerDataCodecInfos));
			obj.add("indexBlockCodecInfo", context.serialize(src.indexBlockCodecInfo));
			obj.add("indexDataCodecInfos", context.serialize(src.indexDataCodecInfos));
			obj.add("indexLocation", context.serialize(src.indexLocation));
			return obj;
		}
	}

}
