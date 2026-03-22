package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

public class MyJsonCache {

	private final MyJsonCacheableContainer container;

	private final ConcurrentHashMap<String, MyCacheInfo> infos = new ConcurrentHashMap<>();

	// TODO: To make memory footprint smaller, we could pass container and path as arguments in each method
	//       (But go with convenience for now...)
	private class MyCacheInfo {

		private final N5GroupPath path;

		MyCacheInfo(N5GroupPath path) {
			this.path = path;
		}

		// TODO: To make memory footprint smaller, could make this a small
		//       Object[] array and implement Map logic on top.
		//       Alternatively, we could split CacheInfo parts into separate Maps, for example,
		//       One Map<String, JsonElement> for containing attributes.json for all paths.
		private final Map<String, JsonElement> attributesCache = new HashMap<>();

		synchronized JsonElement getAttributes(final String attributesKey) throws N5IOException {
			JsonElement attributes = attributesCache.get(attributesKey);
			if (attributes == null && !attributesCache.containsKey(attributesKey)) {
				attributes = container.my_getAttributesFromContainer(path, attributesKey);
				attributesCache.put(attributesKey, attributes);
			}
			return attributes;
		}

		synchronized void setAttributes(final String attributesKey, final JsonElement attributes) {
			attributesCache.put(attributesKey, attributes);
		}
	}

	public MyJsonCache(final MyJsonCacheableContainer container) {
		this.container = container;
	}

	/**
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code group}.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @param group
	 * @param attributesKey
	 *
	 * @return the attributes as a json element.
	 */
	public JsonElement getAttributes(final N5GroupPath group, final String attributesKey) throws N5IOException {

		final MyCacheInfo info = infos.computeIfAbsent(group.path(), k -> new MyCacheInfo(group));
		return info.getAttributes(attributesKey);
	}

	public void updateCachedAttributes(final N5GroupPath group, final String attributesKey, final JsonElement attributes) throws N5IOException {

		final MyCacheInfo info = infos.computeIfAbsent(group.path(), k -> new MyCacheInfo(group));
		info.setAttributes(attributesKey, attributes);
	}

}
