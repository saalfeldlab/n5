package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class N5JsonCache {

	public static final N5CacheInfo emptyCacheInfo = new N5CacheInfo();
	public static final String jsonFile = "attributes.json";

	protected final N5JsonCacheableContainer container;

	/**
	 * Data object for caching meta data.  Elements that are null are not yet
	 * cached.
	 */
	protected static class N5CacheInfo {

		protected final HashMap<String, JsonElement> attributesCache = new HashMap<>();
		protected HashSet<String> children = null;
		protected boolean	isDataset = false;
		protected boolean isGroup = false;

		protected JsonElement getCache(final String normalCacheKey) {

			synchronized (attributesCache) {
				return attributesCache.get(normalCacheKey);
			}
		}

		protected boolean containsKey(final String normalCacheKey) {

			synchronized (attributesCache) {
				return attributesCache.containsKey(normalCacheKey);
			}
		}
	}

	private final HashMap<String, N5JsonCache.N5CacheInfo> containerPathToCache = new HashMap<>();

	public N5JsonCache( final N5JsonCacheableContainer container ) {
		this.container = container;
	}

	public JsonElement getAttributes(final String normalPathKey, final String normalCacheKey) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey, null);
		}
		cacheInfo = getCacheInfo(normalPathKey);
		synchronized (cacheInfo) {
			if (!cacheInfo.containsKey(normalCacheKey)) {
				updateCacheInfo(normalPathKey, normalCacheKey, null);
			}
		}

		final JsonElement output = getCacheInfo(normalPathKey).getCache(normalCacheKey);
		return output == null ? null : output.deepCopy();
	}

	public boolean isDataset(final String normalPathKey, final String normalCacheKey ) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey, null);
		}
		return getCacheInfo(normalPathKey).isDataset;
	}

	public boolean isGroup(final String normalPathKey, String cacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, cacheKey, null);
		}
		return getCacheInfo(normalPathKey).isGroup;
	}

	/**
	 * Returns true if a resource exists.
	 *
	 * @param normalPathKey the container path
	 * @param normalCacheKey the cache key / resource (may be null)
	 * @return true if exists
	 */
	public boolean exists(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo( normalPathKey, normalCacheKey, null);
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

		if( cacheInfo.children == null )
			addChild( cacheInfo, normalPathKey );

		final String[] children = new String[cacheInfo.children.size()];
		int i = 0;
		for (String child : cacheInfo.children) {
			children[i++] = child;
		}
		return children;
	}

	public N5CacheInfo addNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes) {

		final N5CacheInfo cacheInfo;
		if ( container.existsFromContainer(normalPathKey, null)) {
			cacheInfo = newCacheInfo();
		} else {
			cacheInfo = emptyCacheInfo;
		}

		if (cacheInfo != emptyCacheInfo && normalCacheKey != null) {
			final JsonElement attributes = (uncachedAttributes == null)
					? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
					: uncachedAttributes;

			updateCacheAttributes(cacheInfo, normalCacheKey, attributes);
			updateCacheIsGroup(cacheInfo, container.isGroupFromAttributes(normalCacheKey, attributes));
			updateCacheIsDataset(cacheInfo, container.isDatasetFromAttributes(normalCacheKey, attributes));
		} else {
			updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
			updateCacheIsGroup(cacheInfo, container.isDatasetFromContainer(normalPathKey));
		}
		updateCache(normalPathKey, cacheInfo);
		return cacheInfo;
	}

	public N5CacheInfo forceAddNewCacheInfo(String normalPathKey, String normalCacheKey, JsonElement uncachedAttributes, boolean isGroup, boolean isDataset) {

		final N5CacheInfo cacheInfo = newCacheInfo();
		if (normalCacheKey != null) {
			synchronized (cacheInfo.attributesCache) {
				cacheInfo.attributesCache.put(normalCacheKey, uncachedAttributes);
			}
		}
		updateCacheIsGroup(cacheInfo, isGroup);
		updateCacheIsDataset(cacheInfo, isDataset);
		updateCache(normalPathKey, cacheInfo);
		return cacheInfo;
	}

	private N5CacheInfo addNewCacheInfo(String normalPathKey) {

		return addNewCacheInfo(normalPathKey, null, null);
	}

	private void addChild(N5CacheInfo cacheInfo, String normalPathKey) {

		if( cacheInfo.children == null )
			cacheInfo.children = new HashSet<>();

		final String[] children = container.listFromContainer(normalPathKey);
		Collections.addAll(cacheInfo.children, children);
	}

	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey, final JsonElement uncachedAttributes) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null ){
			addNewCacheInfo(normalPathKey, normalCacheKey, uncachedAttributes );
			return;
		} else if (!container.existsFromContainer(normalPathKey, normalCacheKey)) {
			cacheInfo = emptyCacheInfo;
		} else {
			if( cacheInfo == emptyCacheInfo )
				cacheInfo = newCacheInfo();

			if (normalCacheKey != null) {
				final JsonElement attributesToCache = uncachedAttributes == null
						? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
						: uncachedAttributes;

				updateCacheAttributes(cacheInfo, normalCacheKey, attributesToCache);
				updateCacheIsGroup(cacheInfo, container.isGroupFromAttributes(normalCacheKey, attributesToCache));
				updateCacheIsDataset(cacheInfo, container.isDatasetFromAttributes(normalCacheKey, attributesToCache));
			}
			else {
				updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
				updateCacheIsGroup(cacheInfo, container.isDatasetFromContainer(normalPathKey));
			}
		}
		updateCache( normalPathKey, cacheInfo);
	}

	public void setAttributes(final String normalPathKey, final String normalCacheKey, final JsonElement attributes ) {
		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		boolean update = false;
		if (cacheInfo == null ){
			return;
		}

		if( cacheInfo == emptyCacheInfo ) {
			cacheInfo = newCacheInfo();
			update = true;
		}

		updateCacheAttributes(cacheInfo, normalCacheKey, attributes);

		if( update )
			updateCache(normalPathKey, cacheInfo);
	}

	/**
	 * Adds child to the parent's children list, only if the parent has been 
	 * cached, and its children list already exists.
	 *
	 * @param parent parent path
	 * @param child child path
	 */
	public void addChildIfPresent(final String parent, final String child) {

		final N5CacheInfo cacheInfo = getCacheInfo(parent);
		if (cacheInfo == null) return;

		if( cacheInfo.children != null )
			cacheInfo.children.add(child);
	}

	/**
	 * Adds child to the parent's children list, only if the parent has been 
	 * cached, creating a children list if it does not already exist.
	 *
	 * @param parent parent path
	 * @param child child path
	 */
	public void addChild(final String parent, final String child ) {

		final N5CacheInfo cacheInfo = getCacheInfo(parent);
		if (cacheInfo == null) return;

		if( cacheInfo.children == null )
			cacheInfo.children = new HashSet<>();

		cacheInfo.children.add(child);
	}


	public void removeCache(final String normalParentPathKey, final String normalPathKey) {
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, emptyCacheInfo);
			final N5CacheInfo parentCache = containerPathToCache.get(normalParentPathKey);
			if (parentCache != null && parentCache.children != null ) {
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

	protected N5CacheInfo getCacheInfo(String pathKey) {

		synchronized (containerPathToCache) {
			return containerPathToCache.get(pathKey);
		}
	}

	protected N5CacheInfo newCacheInfo() {
		return new N5CacheInfo();
	}

	protected void updateCache(final String normalPathKey, N5CacheInfo cacheInfo) {
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	protected void updateCacheAttributes(final N5CacheInfo cacheInfo, final String normalCacheKey,
			final JsonElement attributes) {
		synchronized (cacheInfo.attributesCache) {
			cacheInfo.attributesCache.put(normalCacheKey, attributes);
		}
	}

	protected void updateCacheIsGroup(final N5CacheInfo cacheInfo, final boolean isGroup) {
		cacheInfo.isGroup = isGroup;
	}

	protected void updateCacheIsDataset(final N5CacheInfo cacheInfo, final boolean isDataset) {
		cacheInfo.isDataset = isDataset;
	}

}
