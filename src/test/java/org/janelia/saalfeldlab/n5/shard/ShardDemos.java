package org.janelia.saalfeldlab.n5.shard;

import java.net.MalformedURLException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

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

}
