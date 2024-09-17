package org.janelia.saalfeldlab.n5.shard;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.nio.ByteOrder;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class ShardDemos {

	public static void main(String[] args) throws MalformedURLException {

		final Path p = Paths.get("src/test/resources/shardExamples/test.zarr/mid_sharded/c/0/0");
		System.out.println(p);

		final String key = p.toString();
		final ShardedDatasetAttributes dsetAttrs = new ShardedDatasetAttributes(
				new long[]{6, 4},
				new int[]{6, 4},
				new int[]{3, 2},
				DataType.UINT8,
				new Codec[]{new N5BlockCodec()},
				new DeterministicSizeCodec[]{new BytesCodec(), new Crc32cChecksumCodec()},
				IndexLocation.END
		);

		final FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess(FileSystems.getDefault());
		final VirtualShard<byte[]> shard = new VirtualShard<>(dsetAttrs, new long[]{0, 0}, kva, key);

		final DataBlock<byte[]> blk = shard.getBlock(0, 0);

		final byte[] data = blk.getData();
		System.out.println(Arrays.toString(data));

		// fill the block with a weird value
		Arrays.fill(data, (byte)123);

		// write the block
		shard.writeBlock(blk);

		// re-read the block and check the data it contains
		final DataBlock<byte[]> blkReread = shard.getBlock(0, 0);
		final byte[] dataReRead = blkReread.getData();
		System.out.println(Arrays.toString(dataReRead));
	}

	@Test
	public void writeReadBlockTest() {

		final N5Factory factory = new N5Factory();
		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.setPrettyPrinting();
		factory.gsonBuilder(gsonBuilder);
		factory.cacheAttributes(false);

		final N5Writer writer = factory.openWriter("src/test/resources/shardExamples/test.n5");

		final ShardedDatasetAttributes datasetAttributes = new ShardedDatasetAttributes(
				new long[]{8, 8},
				new int[]{4, 4},
				new int[]{2, 2},
				DataType.UINT8,
				new Codec[]{new N5BlockCodec(ByteOrder.LITTLE_ENDIAN), new GzipCompression(4)},
				new DeterministicSizeCodec[]{new BytesCodec(ByteOrder.BIG_ENDIAN), new Crc32cChecksumCodec()},
				IndexLocation.END
		);
		writer.createDataset("shard", datasetAttributes);
		writer.deleteBlock("shard", 0, 0);

		final int[] blockSize = datasetAttributes.getBlockSize();
		final DataType dataType = datasetAttributes.getDataType();
		final int numElements = 2 * 2;

		for (int idx1 = 1; idx1 >= 0; idx1--) {
			for (int idx2 = 1; idx2 >= 0; idx2--) {

				final long[] gridPosition = {idx1, idx2};
				final DataBlock<byte[]> dataBlock = (DataBlock<byte[]>)dataType.createDataBlock(blockSize, gridPosition, numElements);
				byte[] data = dataBlock.getData();
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte)((idx1 * 100) + (idx2 * 10) + i);
				}
				writer.writeBlock("shard", datasetAttributes, dataBlock);

				final DataBlock<byte[]> block = (DataBlock<byte[]>)writer.readBlock("shard", datasetAttributes, gridPosition);

				Assert.assertArrayEquals("Read from shard doesn't match", data, block.getData());
			}
		}
	}

}
