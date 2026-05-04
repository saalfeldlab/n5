package org.janelia.saalfeldlab.n5;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

/**
 * {@link N5Reader} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Reader extends N5Reader {

	Gson getGson();

	/**
	 * Get the key for the {@link KeyValueAccess}, that is used for storing attributes.
	 * The N5 format uses "attributes.json".
	 *
	 * @return the attributes key
	 */
	String getAttributesKey();

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return GsonUtils.listAttributes(getAttributes(pathName));
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final JsonElement attributes = getAttributes(normalPath);
		return createDatasetAttributes(attributes);
	}

	default DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		final JsonDeserializationContext context = new JsonDeserializationContext() {

			@Override public <T> T deserialize(JsonElement json, Type typeOfT) throws JsonParseException {

				return getGson().fromJson(json, typeOfT);
			}
		};

		return DatasetAttributes.getJsonAdapter().deserialize(attributes, DatasetAttributes.class, context);
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws N5Exception {

		final String normalPathName = N5URI.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);

		final JsonElement attributes = getAttributes(normalPathName);
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		final String normalPathName = N5URI.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(normalPathName);
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	/**
	 * Reads or the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	JsonElement getAttributes(final String pathName) throws N5Exception;
}
