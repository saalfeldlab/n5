package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5JsonParseException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;

import static org.janelia.saalfeldlab.n5.N5KeyValueReader.ATTRIBUTES_JSON;

/**
 * {@code ContainerDialect} for the N5 format.
 * <ul>
 * <li>Every directory is a group.</li>
 * <li>All attributes of a group/dataset are stored in "attributes.json".</li>
 * <li>Every group that has the mandatory dataset attributes is a dataset.</li>
 * </ul>
 */
public final class N5Dialect implements ContainerDialect {

	private final HierarchyStore store;
	private final Gson gson;

	public N5Dialect(
			final HierarchyStore store,
			final Gson gson) {
		this.store = store;
		this.gson = gson;
	}

	@Override
	public <T> T getAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Type type) throws N5IOException, N5ClassCastException {

		final JsonElement attributes = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
		} catch (JsonIOException | JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}
	}

	@Override
	public DatasetAttributes getDatasetAttributes(
			final N5DirectoryPath path) throws N5IOException {

		final JsonElement attributes = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		return gson.fromJson(attributes, DatasetAttributes.class);
	}

	@Override
	public boolean datasetExists(
			final N5DirectoryPath path) throws N5IOException {

		return getDatasetAttributes(path) != null;
	}

	@Override
	public boolean groupExists(
			final N5DirectoryPath path) throws N5IOException {

		return store.store_isDirectory(path);
	}

	@Override
	public String[] list(
			final N5DirectoryPath group) throws N5IOException {

		return store.store_listDirectories(group);
	}

	@Override
	public Map<String, Class<?>> listAttributes(
			final N5DirectoryPath path) throws N5IOException, N5JsonParseException {

		final JsonElement attributes = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		return GsonUtils.listAttributes(attributes);
	}

	@Override
	public JsonElement getAttributes(
			final N5DirectoryPath path) throws N5IOException {

		return store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
	}

	@Override
	public <T> void setAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final T attribute) throws N5IOException {

		setAttributes(path, Collections.singletonMap(attributePath, attribute));
	}

	@Override
	public void setAttributes(
			final N5DirectoryPath path,
			final Map<String, ?> attributes) throws N5IOException {

		if (!store.store_isDirectory(path))
			throw new N5IOException(String.format("Directory does not exist: %s", path));

		if (attributes == null || attributes.isEmpty())
			return;

		JsonElement root = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		root = root != null && root.isJsonObject()
				? root.getAsJsonObject()
				: new JsonObject();
		root = GsonUtils.insertAttributes(root, attributes, gson);
		store.store_writeAttributesJson(path, ATTRIBUTES_JSON, root, gson);
	}

	@Override
	public boolean removeAttribute(
			final N5DirectoryPath path,
			final String attributePath) throws N5IOException {

		final JsonElement root = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		if (root == null)
			return false;

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		if (null != GsonUtils.removeAttribute(root, normalizedAttributePath)) {
			store.store_writeAttributesJson(path, ATTRIBUTES_JSON, root, gson);
			return true;
		}

		return false;
	}

	@Override
	public <T> T removeAttribute(
			final N5DirectoryPath path,
			final String attributePath,
			final Class<T> clazz) throws N5Exception {

		final JsonElement root = store.store_readAttributesJson(path, ATTRIBUTES_JSON, gson);
		if (root == null)
			return null;

		final String normalizedAttributePath = N5URI.normalizeAttributePath(attributePath);
		final T obj;
		try {
			obj = GsonUtils.removeAttribute(root, normalizedAttributePath, clazz, gson);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5ClassCastException(e);
		}

		if (obj != null)
			store.store_writeAttributesJson(path, ATTRIBUTES_JSON, root, gson);

		return obj;
	}

	@Override
	public void setDatasetAttributes(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		if (!store.store_isDirectory(path))
			store.store_createDirectories(path);
		setAttributes(path, attributes.asMap());
	}

	@Override
	public void createGroup(
			final N5DirectoryPath path) throws N5IOException {

		store.store_createDirectories(path);
	}

	@Override
	public void createDataset(
			final N5DirectoryPath path,
			final DatasetAttributes attributes) throws N5IOException {

		if (!store.store_isDirectory(path))
			store.store_createDirectories(path);
		setAttributes(path, attributes.asMap());
	}

	@Override
	public boolean remove(
			final N5DirectoryPath path) throws N5IOException {

		if (store.store_isDirectory(path))
			store.store_removeDirectory(path);

		// an IOException should have occurred if anything had failed midway
		return true;
	}
}
