package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Reader extends N5Reader {

	@Deprecated
	Gson getGson();

	/**
	 * Get the key for the that is used for storing attributes. The N5 format
	 * uses "attributes.json".
	 *
	 * @return the attributes key
	 */
	@Deprecated
	String getAttributesKey();

	/**
	 * Reads or the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	@Deprecated
	JsonElement getAttributes(String pathName) throws N5Exception;
}
