package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * {@link N5Reader} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Reader extends N5Reader {

	@Deprecated
	default Gson getGson() {
		return getContainerDialect().getGson();
	}

	/**
	 * Get the key for the that is used for storing attributes. The N5 format
	 * uses "attributes.json".
	 *
	 * @return the attributes key
	 */
	@Deprecated
	String getAttributesKey();

	/**
	 * Get the {@code ContainerDialect} of this {@code N5Reader}.
	 * <p>
	 * The {@code ContainerDialect} defines how attributes are stored, what
	 * constitutes a group or a dataset, etc. These details vary between N5,
	 * Zarr v2, and Zarr v3.
	 * <p>
	 * Container hierarchy and metadata management (basically
	 * everything that does not concern {@code DataBlock}s) is delegated to the
	 * {@code ContainerDialect}.
	 *
	 * @return the {@code ContainerDialect}
	 */
	ContainerDialect getContainerDialect();

	/**
	 * Reads the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	@Deprecated
	default JsonElement getAttributes(String pathName) throws N5Exception {

		return getContainerDialect().getAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws N5Exception {

		return getAttribute(pathName, key, TypeToken.get(clazz).getType());
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		return getContainerDialect().getAttribute(N5Path.N5DirectoryPath.of(pathName), key, type);
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().getDatasetAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().listAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	default boolean groupExists(final String pathName) {

		return getContainerDialect().groupExists(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default boolean exists(final String pathName) {

		return groupExists(pathName) || datasetExists(pathName);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		return getContainerDialect().datasetExists(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getContainerDialect().list(N5Path.N5DirectoryPath.of(pathName));
	}
}
