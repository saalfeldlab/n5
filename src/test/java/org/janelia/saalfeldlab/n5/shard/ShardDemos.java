package org.janelia.saalfeldlab.n5.shard;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ShardDemos {

	public static void main(String[] args) throws MalformedURLException {

		final Path p = Paths.get("src/test/resources/shardExamples/test.zarr/mid_sharded/c/0/0");
		System.out.println(p);


		final String key = p.toString();
		final ShardedDatasetAttributes dsetAttrs = new ShardedDatasetAttributes(new long[]{6, 4}, new int[]{6, 4},
				new int[]{3, 2}, IndexLocation.END, DataType.UINT8, new RawCompression(), null);

		final FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess(FileSystems.getDefault());
		final VirtualShard<byte[]> shard = new VirtualShard<>(dsetAttrs, new long[]{0, 0}, kva, key);

		final DataBlock<byte[]> blk = shard.getBlock(0, 0);

		final byte[] data = (byte[])blk.getData();
		System.out.println(Arrays.toString(data));

		// fill the block with a weird value
		Arrays.fill(data, (byte)123);

		// write the block
		shard.writeBlock(blk);

		// re-read the block and check the data it contains
		final DataBlock<byte[]> blkReread = shard.getBlock(0, 0);
		final byte[] dataReRead = (byte[])blkReread.getData();
		System.out.println(Arrays.toString(dataReRead));
	}

	@Test
	public void writeReadBlockTest() {

		final N5Writer writer = N5Factory.createWriter("src/test/resources/shardExamples/test.n5");

		final ShardedDatasetAttributes datasetAttributes = new ShardedDatasetAttributes(
				new long[]{8, 8},
				new int[]{4, 4},
				new int[]{2, 2},
				IndexLocation.END,
				DataType.UINT8,
				new RawCompression(),
				new Codec[]{
						new IdentityCodec(),
						new ShardingCodec(
								new ShardingConfiguration(
										new int[]{2, 2},
										new Codec[]{new Compression.CompressionCodec(new RawCompression()), new IdentityCodec()},
										new Codec[]{new Crc32cChecksumCodec()},
										IndexLocation.END)
						)
				}
		);
		writer.createDataset("shard", datasetAttributes);
		final DataBlock<?> dataBlock = datasetAttributes.getDataType().createDataBlock(datasetAttributes.getBlockSize(), new long[]{0, 0}, 2 * 2);

		writer.writeBlock("shard", datasetAttributes, dataBlock);
		writer.readBlock("shard", datasetAttributes, 0,0);
	}

	private static class ZarrConfig<T> {
		final String name;
		final T configuration;

		private ZarrConfig() {
			name = "";
			configuration = null;
		}
	}

	private class GridConfig<T> extends ZarrConfig<T> {}
	private class KeyEncodingConfig<T> extends ZarrConfig<T> {}

	private class ZarrChunk {}

	private class ZarrChunkAdapter implements com.google.gson.JsonSerializer<ZarrChunk>, com.google.gson.JsonDeserializer<ZarrChunk> {
		final ZarrConfig<GridConfig> grid;
		final ZarrConfig<KeyEncodingConfig> keyEncoding;

		public ZarrChunkAdapter() {
			grid = null;
			keyEncoding = null;
		}
		public ZarrChunkAdapter(ZarrConfig<GridConfig> grid, ZarrConfig<KeyEncodingConfig> key_encoding) {

			this.grid = grid;
			this.keyEncoding = key_encoding;
		}

		@Override public ZarrChunk deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

			if (!json.isJsonObject()) return null;

			final JsonObject obj = json.getAsJsonObject();
			final JsonObject grid = obj.getAsJsonObject("chunk_grid");

			return null;
		}

		@Override public JsonElement serialize(ZarrChunk src, Type typeOfSrc, JsonSerializationContext context) {

			return null;
		}
	}


	@Test
	public void nameConfigurationGsonTest() {

		final N5Factory factory = new N5Factory();
		final GsonBuilder gson = new GsonBuilder();


		gson.registerTypeHierarchyAdapter()
		factory.gsonBuilder(gson);
		final N5Reader n5 = factory.openReader("src/test/resources/shardExamples/test.zarr/mid_sharded");

		final JsonObject zarrJson = n5.getAttribute("/", "/", JsonObject.class);
		zarrJson.remove("shard")
	}

}
