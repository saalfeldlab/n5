package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;

public interface N5ShardReader extends N5Reader {

	/**
	 * Reads the {@link Shard} at the corresponding grid position.
	 * 
	 * @param <A>
	 * @param datasetPath
	 * @param datasetAttributes
	 * @param shardGridPosition
	 * @return the shard
	 */
	public <A extends DatasetAttributes & ShardParameters> Shard<?> readShard(final String datasetPath,
			final A datasetAttributes, long... shardGridPosition);

	// TODO Caleb suggested this
//	public <A extends DatasetAttributes & ShardParameters> DataBlock<?> readBlockFromShard(
//			final String datasetPath, final A datasetAttributes, long... blockGridPosition);

}
