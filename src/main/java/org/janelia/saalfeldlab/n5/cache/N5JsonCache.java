package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class N5JsonCache {

	public static final N5CacheInfo emptyCacheInfo = new N5CacheInfo();
	public static final String jsonFile = "attributes.json";

	private final N5JsonCacheableContainer container;

	/**
	 * Data object for caching meta data.  Elements that are null are not yet
	 * cached.
	 */
	private static class N5CacheInfo {

		private final HashSet<String> children = new HashSet<>();

		private final HashMap<String, JsonElement> attributesCache = new HashMap<>();
		private Boolean isDataset = false;
		private Boolean isGroup = false;

		private JsonElement getCache(final String normalCacheKey) {

			synchronized (attributesCache) {
				return attributesCache.get(normalCacheKey);
			}
		}

		private boolean containsKey(final String normalCacheKey) {

			synchronized (attributesCache) {
				return attributesCache.containsKey(normalCacheKey);
			}
		}
	}

	private final HashMap<String, N5JsonCache.N5CacheInfo> containerPathToCache = new HashMap<>();

	public N5JsonCache( final N5JsonCacheableContainer container ) {
		this.container = container;
	}

	/**
	 * Get cached attributes for the group identified by a normalPath
	 *
	 * @param cacheInfo
	 * @param normalCacheKey normalized cache key
	 * @return cached attributes
	 * null if the group exists but no attributes are set, the group does not exist, or if attributes have not been cached
	 */
	private JsonElement getCachedAttributes(final N5CacheInfo cacheInfo, final String normalCacheKey) {
		return cacheInfo == null ? null : cacheInfo.getCache(normalCacheKey);
	}

	public JsonElement getAttributes(final String normalPathKey, final String normalCacheKey) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			cacheAttributes(normalPathKey, normalCacheKey);
		}
		cacheInfo = getCacheInfo(normalPathKey);
		synchronized (cacheInfo) {
			if (!cacheInfo.containsKey(normalCacheKey)) {
				updateCacheInfo(normalPathKey, normalCacheKey, null);
			}
		}

		return getCacheInfo(normalPathKey).getCache(normalCacheKey);
	}

	public boolean isDataset(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey);
		}
		return getCacheInfo(normalPathKey).isDataset;
	}

	public boolean isGroup(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey);
		}
		return getCacheInfo(normalPathKey).isGroup;
	}

	public boolean exists(final String normalPathKey, final String normalCacheKey ) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey );
		}
		return getCacheInfo(normalPathKey) != emptyCacheInfo;
	}

	public String[] list(String normalPathKey) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey);
		}
		cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == emptyCacheInfo) return null;

		final String[] children = new String[cacheInfo.children.size()];
		int i = 0;
		for (String child : cacheInfo.children) {
			children[i++] = child;
		}
		return children;
	}

	private void cacheAttributes(final String normalPathKey, final String normalCacheKey) {

		final JsonElement uncachedValue = container.getAttributesFromContainer(normalPathKey, normalCacheKey);
		if (uncachedValue != null || container.existsFromContainer(normalPathKey)) {
			addNewCacheInfo(normalPathKey, normalCacheKey, uncachedValue);
		}
	}

	private void addNewCacheInfo(String normalPathKey, String normalCacheKey) {

		addNewCacheInfo( normalPathKey, normalCacheKey, container.getAttributesFromContainer( normalPathKey, normalCacheKey));
	}

	private void addNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes) {

		final N5CacheInfo cacheInfo;
		if (!container.existsFromContainer(normalPathKey)) {
			cacheInfo = emptyCacheInfo;
		} else {
			cacheInfo = new N5CacheInfo();
			synchronized (cacheInfo.attributesCache) {
				cacheInfo.attributesCache.put(normalCacheKey, uncachedAttributes);
			}
			cacheInfo.isGroup = container.isGroupFromContainer(normalPathKey);
			cacheInfo.isDataset = container.isDatasetFromContainer(normalPathKey);
			addChild(cacheInfo, normalPathKey);
		}
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	public void addNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes, boolean isGroup, boolean isDataset) {

		final N5CacheInfo cacheInfo = new N5CacheInfo();
		synchronized (cacheInfo.attributesCache) {
			cacheInfo.attributesCache.put(normalCacheKey, uncachedAttributes);
		}
		cacheInfo.isGroup = isGroup;
		cacheInfo.isDataset = isDataset;
		addChild(cacheInfo, normalPathKey);
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	private void addChild(N5CacheInfo cacheInfo, String normalPathKey) {

		final String[] children = container.listFromContainer(normalPathKey);
		Collections.addAll(cacheInfo.children, children);
	}

	private N5CacheInfo addNewCacheInfo(String normalPathKey) {

		final N5CacheInfo cacheInfo;
		if (!container.existsFromContainer(normalPathKey)) {
			cacheInfo = emptyCacheInfo;
		} else {
			cacheInfo = new N5CacheInfo();
			cacheInfo.isGroup = container.isGroupFromContainer(normalPathKey);
			cacheInfo.isDataset = container.isDatasetFromContainer(normalPathKey);
			addChild(cacheInfo, normalPathKey);
		}
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
		return cacheInfo;
	}

	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null ){
			addNewCacheInfo(normalPathKey);
			return;
		} else if (!container.existsFromContainer(normalPathKey)) {
			cacheInfo = emptyCacheInfo;
		} else {
			if( cacheInfo == emptyCacheInfo )
				cacheInfo = new N5CacheInfo();

			final JsonElement attributesToCache = container.getAttributesFromContainer(normalPathKey, normalCacheKey);
			synchronized (cacheInfo.attributesCache) {
				cacheInfo.attributesCache.put(normalCacheKey, attributesToCache);
			}
			cacheInfo.isGroup = container.isGroupFromContainer(normalPathKey);
			cacheInfo.isDataset = container.isDatasetFromContainer(normalPathKey);
			cacheInfo.children.clear();
			addChild(cacheInfo, normalPathKey);
		}
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey, final JsonElement uncachedAttributes) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null ){
			addNewCacheInfo(normalPathKey, normalCacheKey, uncachedAttributes );
			return;
		} else if (!container.existsFromContainer(normalPathKey)) {
			cacheInfo = emptyCacheInfo;
		} else {
			if( cacheInfo == emptyCacheInfo )
				cacheInfo = new N5CacheInfo();

			final JsonElement attributesToCache = uncachedAttributes == null ? container.getAttributesFromContainer(normalPathKey, normalCacheKey) : uncachedAttributes;
			synchronized (cacheInfo.attributesCache) {
				cacheInfo.attributesCache.put(normalCacheKey, attributesToCache);
			}
			cacheInfo.isGroup = container.isGroupFromContainer(normalPathKey);
			cacheInfo.isDataset = container.isDatasetFromContainer(normalPathKey);
			cacheInfo.children.clear();
			addChild(cacheInfo, normalPathKey);
		}

		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	public void setIsDataset(final String normalPathKey, final boolean isDataset ) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		boolean update = false;
		if (cacheInfo == null ){
			return;
		}
		if( cacheInfo == emptyCacheInfo ) {
			cacheInfo = new N5CacheInfo();
			update = true;
		}

		synchronized (cacheInfo) {
			cacheInfo.isDataset = isDataset;
		}

		if( update )
			synchronized (containerPathToCache) {
				containerPathToCache.put(normalPathKey, cacheInfo);
			}
	}

	public void setAttributes(final String normalPathKey, final String normalCacheKey, final JsonElement attributes ) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		boolean update = false;
		if (cacheInfo == null ){
			return;
		}

		if( cacheInfo == emptyCacheInfo ) {
			cacheInfo = new N5CacheInfo();
			update = true;
		}

		synchronized (cacheInfo.attributesCache) {
			cacheInfo.attributesCache.put(normalCacheKey, attributes);
		}

		if( update )
			synchronized (containerPathToCache) {
				containerPathToCache.put(normalPathKey, cacheInfo);
			}
	}

	public void addChild(final String parent, final String child) {

		final N5CacheInfo cacheInfo = getCacheInfo(parent);
		if (cacheInfo == null) return;
		cacheInfo.children.add(child);
	}

	public void removeCache(final String normalParentPathKey, final String normalPathKey) {
		synchronized (containerPathToCache) {
			containerPathToCache.remove(normalPathKey);
			final N5CacheInfo parentCache = containerPathToCache.get(normalParentPathKey);
			if (parentCache != null) {
				parentCache.children.remove(normalPathKey);
			}
		}
	}
	public void clearCache(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo != null && cacheInfo != emptyCacheInfo) {
			synchronized (cacheInfo.attributesCache) {
				cacheInfo.attributesCache.remove(normalCacheKey);
			}
		}
	}

	private N5CacheInfo getCacheInfo(String pathKey) {

		synchronized (containerPathToCache) {
			return containerPathToCache.get(pathKey);
		}
	}

}
