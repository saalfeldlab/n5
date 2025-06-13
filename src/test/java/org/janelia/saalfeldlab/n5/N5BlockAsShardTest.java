package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.shard.InMemoryShard;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardIndex;


public class N5BlockAsShardTest extends N5FSTest {

	@Override protected N5Writer createN5Writer(String location, GsonBuilder gson) {

		return new N5FSWriter(location, gson) {

			@Override public <T> void writeBlock(String path, DatasetAttributes datasetAttributes, DataBlock<T> dataBlock) throws N5Exception {

				final ShardIndex index = datasetAttributes.getShardingCodec().createIndex(datasetAttributes);
				final InMemoryShard<T> shard = new InMemoryShard<T>(datasetAttributes, dataBlock.getGridPosition(), index);
				shard.addBlock(dataBlock);
				writeShard(path, datasetAttributes, shard);
			}

			@Override public <T> DataBlock<T> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) throws N5Exception {

				final Shard<T> shard = readShard(pathName, datasetAttributes, gridPosition);
				if (shard == null)
					return null;

				return shard.getBlock(gridPosition);
			}
		};
	}

	@Override protected N5Reader createN5Reader(String location, GsonBuilder gson) {

		return new N5FSReader(location, gson) {

			@Override public <T> DataBlock<T> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) throws N5Exception {

				final Shard<T> shard = readShard(pathName, datasetAttributes, gridPosition);
				if (shard == null)
					return null;

				return shard.getBlock(gridPosition);
			}
		};
	}
}
