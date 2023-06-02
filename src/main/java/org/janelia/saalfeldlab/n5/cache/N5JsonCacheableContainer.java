package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;

/**
 * An N5 container whose structure and attributes can be cached.
 * <p>
 * Implementations of interface methods must explicitly query the backing
 * storage. Cached implmentations (e.g {@link CachedGsonKeyValueN5Reader}) call
 * these methods to update their {@link N5JsonCache}. Corresponding
 * {@link N5Reader} methods should use the cache, if present.
 */
public interface N5JsonCacheableContainer {

	/**
	 * Returns a {@link JsonElement} containing attributes at a given path, 
	 * for a given cache key.
	 *
	 * @param normalPathName the normalized path name
	 * @param normalCacheKey the cache key
	 * @return the attributes as a json element. 
	 * @see GsonKeyValueN5Reader#getAttributes
	 */
	JsonElement getAttributesFromContainer(final String normalPathName, final String normalCacheKey);

	/**
	 * Query whether a resource exists in this container.
	 * 
	 * @param normalPathName the normalized path name
	 * @param normalCacheKey the normalized resource name (may be null).
	 * @return true if the resouce exists
	 */
	boolean existsFromContainer(final String normalPathName, final String normalCacheKey);

	/**
	 * Query whether a path in this container is a group.
	 * 
	 * @param normalPathName the normalized path name
	 * @return true if the path is a group
	 */
	boolean isGroupFromContainer(final String normalPathName);

	/**
	 * Query whether a path in this container is a dataset.
	 * 
	 * @param normalPathName the normalized path name
	 * @return true if the path is a dataset
	 * @see N5Reader#datasetExists
	 */
	boolean isDatasetFromContainer(final String normalPathName);

	/**
	 * If this method is called, its parent path must exist,
	 * and normalCacheKey must exist, with contents given by attributes.
	 *
	 * @param normalCacheKey the cache key
	 * @param attributes the attributes
	 * @return true if the path is a group
	 */
	boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes);

	/**
	 * If this method is called, its parent path must exist,
	 * and normalCacheKey must exist, with contents given by attributes.
	 *
	 * @param normalCacheKey the cache key
	 * @param attributes the attributes
	 * @return true if the path is a dataset
	 */
	boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes);

	/**
	 * List the children of a path for this container.
	 *
	 * @param normalPathName the normalized path name
	 * @return list of children
	 * @see N5Reader#list
	 */
	String[] listFromContainer(final String normalPathName);

}
