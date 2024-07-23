package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.shard.ShardingConfiguration.IndexLocation;

public class ShardReader {


	private final ShardedDatasetAttributes datasetAttributes;
	private long[] indexes;
	private Shards shards;

	public ShardReader(final ShardedDatasetAttributes datasetAttributes) {

		this.datasetAttributes = datasetAttributes;
		this.shards = new Shards(datasetAttributes);
	}

	public ShardIndex readIndexes(FileChannel channel) throws IOException {

		return ShardIndex.read(channel, datasetAttributes);
	}

	public InMemoryShard readShardFully(
			final FileChannel channel,
			long... gridPosition) throws IOException {

		final DatasetAttributes dsetAttrs = shards.getDatasetAttributes();

		final ShardIndex si = readIndexes(channel);
		return null;
	}

	public DataBlock<?> readBlock(
			final FileChannel in,
			long... blockPosition) throws IOException {

		// TODO generalize from FileChannel
		// TODO this assumes the "file" holding the shard is known,
		// the logic to figure that out will have to go somewhere

		final ShardIndex index = readIndexes(in);

		final long[] shardPosition = shards.getShardPositionForBlock(blockPosition);
		in.position(index.getOffset(shardPosition));
		final InputStream is = Channels.newInputStream(in);
		return DefaultBlockReader.readBlock(is, datasetAttributes, indexes);
	}

	private long getIndexIndex(long... shardPosition) {

		final int[] indexDimensions = shards.getShardBlockGridSize();
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
	}

	private static ShardedDatasetAttributes buildTestAttributes() {

		final Codec[] codecs = new Codec[]{
				new ShardingCodec(new ShardingConfiguration(new int[]{2, 2}, null, null, IndexLocation.end))};

		return new ShardedDatasetAttributes(new long[]{4, 4}, new int[]{2, 2}, DataType.INT32, new RawCompression(), codecs);
	}

}
