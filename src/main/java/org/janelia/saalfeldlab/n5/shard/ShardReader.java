package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class ShardReader {

	private final ShardedDatasetAttributes datasetAttributes;
	private long[] indexes;

	public ShardReader(final ShardedDatasetAttributes datasetAttributes) {

		this.datasetAttributes = datasetAttributes;
	}

	public ShardIndex readIndexes(FileChannel channel) throws IOException {

		return ShardIndex.read(channel, datasetAttributes);
	}

	public DataBlock<?> readBlock(
			final FileChannel in,
			long... blockPosition) throws IOException {

		throw new IOException("Remove this!");
	}

	private long getIndexIndex(long... shardPosition) {

		final int[] indexDimensions = datasetAttributes.getBlocksPerShard();
		long idx = 0;
		for (int i = 0; i < indexDimensions.length; i++) {
			idx += shardPosition[i] * indexDimensions[i];
		}

		return idx;
	}

	public static void main(String[] args) {

		final ShardReader reader = new ShardReader(buildTestAttributes());

		System.out.println(reader.getIndexIndex(0, 0));
		System.out.println(reader.getIndexIndex(0, 1));
		System.out.println(reader.getIndexIndex(1, 0));
		System.out.println(reader.getIndexIndex(1, 1));

		final N5Reader n5 = new N5FSReader("shard.n5");
		final ShardedDatasetAttributes datasetAttributes = buildTestAttributes();
		n5.readBlock("dataset", datasetAttributes, 0, 0, 0);

	}

	private static ShardedDatasetAttributes buildTestAttributes() {

		return new ShardedDatasetAttributes(
				new long[]{4, 4},
				new int[]{2, 2},
				new int[]{2, 2},
				DataType.INT32,
				new Codec[]{new N5BlockCodec(), new IdentityCodec()},
				new DeterministicSizeCodec[]{new Crc32cChecksumCodec()},
				IndexLocation.END);
	}

}
