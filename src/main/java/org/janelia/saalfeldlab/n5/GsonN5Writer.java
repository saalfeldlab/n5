package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Writer} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Writer extends GsonN5Reader, N5Writer {

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
