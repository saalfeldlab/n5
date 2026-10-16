package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.Map;

/**
 * {@link N5Writer} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Writer extends GsonN5Reader, N5Writer {

	@Override
	default void createGroup(final String path) throws N5Exception {

		getContainerDialect().createGroup(N5Path.N5DirectoryPath.of(path));
	}

	@Override
	default DatasetAttributes createDataset(final String datasetPath, final DatasetAttributes datasetAttributes) throws N5Exception {

		final DatasetAttributes attributes = getConvertedDatasetAttributes(datasetAttributes);
		getContainerDialect().createDataset(N5Path.N5DirectoryPath.of(datasetPath), attributes);
		return attributes;
	}

	@Override
	default void setAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		getContainerDialect().setAttributes(N5Path.N5DirectoryPath.of(path), attributes);
	}

	@Override
	default boolean removeAttribute(final String path, final String attributePath) throws N5Exception {

		return getContainerDialect().removeAttribute(N5Path.N5DirectoryPath.of(path), attributePath);
	}

	@Override
	default <T> T removeAttribute(final String path, final String attributePath, final Class<T> clazz) throws N5Exception {

		return getContainerDialect().removeAttribute(N5Path.N5DirectoryPath.of(path), attributePath, clazz);
	}

	@Override
	default void setDatasetAttributes(final String datasetPath, final DatasetAttributes datasetAttributes) throws N5Exception {

		final DatasetAttributes attributes = getConvertedDatasetAttributes(datasetAttributes);
		getContainerDialect().setDatasetAttributes(N5Path.N5DirectoryPath.of(datasetPath), attributes );
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		return getContainerDialect().remove(N5Path.N5DirectoryPath.of(path));
	}

	/**
	 * Set the attributes of a group. This result of this method is equivalent
	 * with {@link N5Writer#setAttribute(String, String, Object) N5Writer#setAttribute(groupPath, "/", attributes)}.
	 *
	 * @param groupPath
	 *            to write the attributes to
	 * @param attributes
	 *            to write
	 * @throws N5Exception if the attributes cannot be set
	 */
	@Deprecated
	default void setAttributes(
			final String groupPath,
			final JsonElement attributes) throws N5Exception {
		setAttribute(groupPath,"/", attributes);
	}
}
