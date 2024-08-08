package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.IdentityCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

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

		// TODO generalize from FileChannel
		// TODO this assumes the "file" holding the shard is known,
		// the logic to figure that out will have to go somewhere

		final ShardIndex index = readIndexes(in);

		final long[] shardPosition = datasetAttributes.getShardPositionForBlock(blockPosition);
		in.position(index.getOffset(shardPosition));
		final InputStream is = Channels.newInputStream(in);
		return DefaultBlockReader.readBlock(is, datasetAttributes, indexes);
	}

	private long getIndexIndex(long... shardPosition) {

		final int[] indexDimensions = datasetAttributes.getShardBlockGridSize();
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

		final Codec[] codecs = new Codec[]{
				new IdentityCodec(),
				new ShardingCodec(
						new ShardingConfiguration(
								new int[]{2, 2},
								new Codec[]{new RawCompression(), new IdentityCodec()},
								new Codec[]{new Crc32cChecksumCodec()},
								IndexLocation.END)
				)
		};

		return new ShardedDatasetAttributes(new long[]{4, 4}, new int[]{2, 2}, new int[]{2, 2}, IndexLocation.END, DataType.INT32, new RawCompression(), codecs);
	}

}
