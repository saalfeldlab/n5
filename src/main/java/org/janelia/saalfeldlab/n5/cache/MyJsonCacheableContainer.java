package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

public interface MyJsonCacheableContainer {

	/**
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code group}.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @return the attributes as a json element.
	 */
	JsonElement my_getAttributesFromContainer(N5GroupPath group, String attributesKey) throws N5IOException;

}
