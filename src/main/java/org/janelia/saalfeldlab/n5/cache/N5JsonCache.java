package org.janelia.saalfeldlab.n5.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.janelia.saalfeldlab.n5.N5Exception;

import com.google.gson.JsonElement;

/*
 * A cache containing JSON attributes and children for groups and
 * datasets stored in N5 containers. Used by {@link CachedGsonKeyValueN5Reader}
 * and {@link CachedGsonKeyValueN5Writer}.
 *
 */
public class N5JsonCache {

	public static final N5CacheInfo emptyCacheInfo = new N5CacheInfo();

	public static final EmptyJson emptyJson = new EmptyJson();

	protected final N5JsonCacheableContainer container;

	/**
	 * Data object for caching meta data. Elements that are null are not yet
	 * cached.
	 */
	protected static class N5CacheInfo {

		protected final HashMap<String, JsonElement> attributesCache = new HashMap<>();
		protected HashSet<String> children = null;
		protected boolean isDataset = false;
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

	protected static class EmptyJson extends JsonElement {

		@Override
		public JsonElement deepCopy() {

			throw new N5Exception("Do not copy EmptyJson, you naughty person");
		}

	}

	private final HashMap<String, N5JsonCache.N5CacheInfo> containerPathToCache = new HashMap<>();

	public N5JsonCache(final N5JsonCacheableContainer container) {

		this.container = container;
	}

	public JsonElement getAttributes(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey, null);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		if (cacheInfo == emptyCacheInfo || cacheInfo.getCache(normalCacheKey) == emptyJson) {
			return null;
		}
		synchronized (cacheInfo) {
			if (!cacheInfo.containsKey(normalCacheKey)) {
				updateCacheInfo(normalPathKey, normalCacheKey, null);
			}
		}

		final JsonElement output = cacheInfo.getCache(normalCacheKey);
		return output == null ? null : output.deepCopy();
	}

	public boolean isDataset(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey, null);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		return cacheInfo.isDataset;
	}

	public boolean isGroup(final String normalPathKey, final String cacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, cacheKey, null);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		return cacheInfo.isGroup;
	}

	/**
	 * Returns true if a resource exists.
	 *
	 * @param normalPathKey
	 *            the container path
	 * @param normalCacheKey
	 *            the cache key / resource (may be null)
	 * @return true if exists
	 */
	public boolean exists(final String normalPathKey, final String normalCacheKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey, normalCacheKey, null);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		return cacheInfo != emptyCacheInfo;
	}

	public String[] list(final String normalPathKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			addNewCacheInfo(normalPathKey);
			cacheInfo = getCacheInfo(normalPathKey);
		}
		if (cacheInfo == emptyCacheInfo)
			throw new N5Exception.N5IOException(normalPathKey + " is not a valid group");

		if (cacheInfo.children == null)
			addChild(cacheInfo, normalPathKey);

		final String[] children = new String[cacheInfo.children.size()];
		int i = 0;
		for (final String child : cacheInfo.children) {
			children[i++] = child;
		}
		return children;
	}

	public N5CacheInfo addNewCacheInfo(
			final String normalPathKey,
			final String normalCacheKey,
			final JsonElement uncachedAttributes) {

		final N5CacheInfo cacheInfo;
		if (container.existsFromContainer(normalPathKey, normalCacheKey)) {
			cacheInfo = newCacheInfo();
		} else {
			cacheInfo = emptyCacheInfo;
		}

		if (cacheInfo != emptyCacheInfo) {
			if (normalCacheKey != null) {
				final JsonElement attributes = (uncachedAttributes == null)
						? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
						: uncachedAttributes;

				updateCacheAttributes(cacheInfo, normalCacheKey, attributes);
				updateCacheIsGroup(cacheInfo, container.isGroupFromAttributes(normalCacheKey, attributes));
				updateCacheIsDataset(cacheInfo, container.isDatasetFromAttributes(normalCacheKey, attributes));
			} else {
				updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
				updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
			}
		}
		updateCache(normalPathKey, cacheInfo);
		return cacheInfo;
	}

	private N5CacheInfo addNewCacheInfo(final String normalPathKey) {

		return addNewCacheInfo(normalPathKey, null, null);
	}

	private void addChild(final N5CacheInfo cacheInfo, final String normalPathKey) {

		if (cacheInfo.children == null)
			cacheInfo.children = new HashSet<>();

		final String[] children = container.listFromContainer(normalPathKey);
		Collections.addAll(cacheInfo.children, children);
	}

	protected N5CacheInfo getOrMakeCacheInfo(final String normalPathKey) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null) {
			return addNewCacheInfo(normalPathKey, null, null);
		}

		if (cacheInfo == emptyCacheInfo)
			cacheInfo = newCacheInfo();

		return cacheInfo;
	}

	/**
	 * Updates the cache attributes for the given normalPathKey
	 * adding the appropriate node to the cache if necessary.
	 *
	 * @param normalPathKey
	 *            the normalized path key
	 * @param normalCacheKey
	 * 			the normalize key to cache
	 */
	public void updateCacheInfo(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getOrMakeCacheInfo(normalPathKey);
		final JsonElement attrs = cacheInfo.attributesCache.get(normalCacheKey);
		updateCacheInfo(normalPathKey, normalCacheKey, attrs);
	}

	/**
	 * Updates the cache attributes for the given normalPathKey and
	 * normalCacheKey,
	 * adding the appropriate node to the cache if necessary.
	 *
	 * @param normalPathKey
	 *            the normalized path key
	 * @param normalCacheKey
	 *            the normalized cache key
	 * @param uncachedAttributes
	 *            attributes to be cached
	 */
	public void updateCacheInfo(
			final String normalPathKey,
			final String normalCacheKey,
			final JsonElement uncachedAttributes) {

		final N5CacheInfo cacheInfo = getOrMakeCacheInfo(normalPathKey);
		if (normalCacheKey != null) {
			final JsonElement attributesToCache = uncachedAttributes == null
					? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
					: uncachedAttributes;

			updateCacheAttributes(cacheInfo, normalCacheKey, attributesToCache);
			updateCacheIsGroup(cacheInfo, container.isGroupFromAttributes(normalCacheKey, attributesToCache));
			updateCacheIsDataset(cacheInfo, container.isDatasetFromAttributes(normalCacheKey, attributesToCache));
		} else {
			updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
			updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
		}
		updateCache(normalPathKey, cacheInfo);
	}

	public void initializeNonemptyCache(final String normalPathKey, final String normalCacheKey) {

		final N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null || cacheInfo == emptyCacheInfo) {
			final N5CacheInfo info = newCacheInfo();
			if (normalCacheKey != null)
				info.attributesCache.put(normalCacheKey, emptyJson);

			updateCache(normalPathKey, info);
		}
	}

	public void setAttributes(final String normalPathKey, final String normalCacheKey, final JsonElement attributes) {

		N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		boolean update = false;
		if (cacheInfo == null) {
			return;
		}

		if (cacheInfo == emptyCacheInfo) {
			cacheInfo = newCacheInfo();
			update = true;
		}

		updateCacheAttributes(cacheInfo, normalCacheKey, attributes);

		if (update)
			updateCache(normalPathKey, cacheInfo);
	}

	/**
	 * Adds child to the parent's children list, only if the parent has been
	 * cached, and its children list already exists.
	 *
	 * @param parent
	 *            parent path
	 * @param child
	 *            child path
	 */
	public void addChildIfPresent(final String parent, final String child) {

		final N5CacheInfo cacheInfo = getCacheInfo(parent);
		if (cacheInfo == null)
			return;

		if (cacheInfo.children != null)
			cacheInfo.children.add(child);
	}

	/**
	 * Adds child to the parent's children list, only if the parent has been
	 * cached, creating a children list if it does not already exist.
	 *
	 * @param parent
	 *            parent path
	 * @param child
	 *            child path
	 */
	public void addChild(final String parent, final String child) {

		final N5CacheInfo cacheInfo = getCacheInfo(parent);
		if (cacheInfo == null)
			return;

		if (cacheInfo.children == null)
			cacheInfo.children = new HashSet<>();

		cacheInfo.children.add(child);
	}

	public void removeCache(final String normalParentPathKey, final String normalPathKey) {

		// this path and all children should be removed = set to emptyCacheInfo
		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, emptyCacheInfo);
			containerPathToCache.keySet().stream().filter(x -> {
				return x.startsWith(normalPathKey + "/");
			}).forEach(x -> {
				containerPathToCache.put(x, emptyCacheInfo);
			});
		}

		// update the parent's children, if present (remove the normalPathKey)
		final N5CacheInfo parentCache = containerPathToCache.get(normalParentPathKey);
		if (parentCache != null && parentCache.children != null) {
			parentCache.children.remove(normalPathKey.replaceFirst(normalParentPathKey + "/", ""));
		}
	}

	protected N5CacheInfo getCacheInfo(final String pathKey) {

		synchronized (containerPathToCache) {
			return containerPathToCache.get(pathKey);
		}
	}

	protected N5CacheInfo newCacheInfo() {

		return new N5CacheInfo();
	}

	protected void updateCache(final String normalPathKey, final N5CacheInfo cacheInfo) {

		synchronized (containerPathToCache) {
			containerPathToCache.put(normalPathKey, cacheInfo);
		}
	}

	protected void updateCacheAttributes(
			final N5CacheInfo cacheInfo,
			final String normalCacheKey,
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
