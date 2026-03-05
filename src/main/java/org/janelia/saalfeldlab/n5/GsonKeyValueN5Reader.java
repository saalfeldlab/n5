package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
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

	ContainerDialect getContainerDialect();

	@Override
	default URI getURI() {

		return getKeyValueRoot().uri();
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		return getContainerDialect().getAttribute(N5DirectoryPath.of(pathName), key, type);
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().getDatasetAttributes(N5DirectoryPath.of(pathName));
	}

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().listAttributes(N5DirectoryPath.of(pathName));
	}

	default boolean groupExists(final String pathName) {

		return getContainerDialect().groupExists(N5DirectoryPath.of(pathName));
	}

	@Override
	default boolean exists(final String pathName) {

		return groupExists(pathName) || datasetExists(pathName);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		return getContainerDialect().datasetExists(N5DirectoryPath.of(pathName));
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getContainerDialect().list(N5DirectoryPath.of(pathName));
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


	// ------------------------------------------------------------------------
	//
	// -- deprecated / semi-obsolete --
	//

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5Exception if the attributes cannot be read
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5Exception { // TODO: deprecate?

		return getContainerDialect().getAttributes(N5DirectoryPath.of(pathName));
	}

	@Deprecated
	default KeyValueAccess getKeyValueAccess() {
		return getKeyValueRoot().getKVA();
	}
}
