package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.shard.InMemoryShard;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;

public interface N5ShardWriter extends N5Writer {

	/**
	 * Writes a {@link Shard}.
	 * <p>
	 * Any 
	 *
	 * @param datasetPath dataset path
	 * @param datasetAttributes the dataset attributes
	 * @param shard the shard
	 * @param <T> the data block data type
	 * @param <A> the attribute type
	 * @throws N5Exception the exception
	 */
	<T,A extends DatasetAttributes & ShardParameters> void writeShard(
			final String datasetPath,
			final A datasetAttributes,
			final Shard<T> shard) throws N5Exception;

	// TODO Caleb suggested something like this
//	public <A extends DatasetAttributes & ShardParameters> DataBlock<?> writeBlockWithShard(
//			final String datasetPath, final A datasetAttributes, DataBlock<?> block);

}
