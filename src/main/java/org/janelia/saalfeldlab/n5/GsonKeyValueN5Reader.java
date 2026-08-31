package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import java.net.URI;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader {

	KeyValueRoot getKeyValueRoot();

	@Deprecated
	default KeyValueAccess getKeyValueAccess() {
		return getKeyValueRoot().getKVA();
	}

	@Override
	default URI getURI() {

		return getKeyValueRoot().uri();
	}

	@Override
	default <T> DataBlock<T> readChunk(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readChunk(pva, gridPosition);

		} catch (N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default <T> List<DataBlock<T>> readChunks(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final List<long[]> blockPositions) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(pathName), convertedDatasetAttributes);
		return convertedDatasetAttributes.<T> getDatasetAccess().readChunks(pva, blockPositions);
	}

	@Override
	default <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final int shardLevel = convertedDatasetAttributes.getNestedBlockGrid().numLevels() - 1;
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(pathName),
					convertedDatasetAttributes);
			return convertedDatasetAttributes.<T> getDatasetAccess().readBlock(pva, gridPosition, shardLevel);

		} catch (N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default boolean blockExists(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final N5Path path = N5DirectoryPath.of(pathName).resolve(datasetAttributes.relativeBlockPath(gridPosition));
		return getKeyValueRoot().isFile(path);
	}

}
