package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import org.janelia.saalfeldlab.n5.cache.HierarchyCache;

/**
 * {@link N5Reader} implementation through {@link KeyValueRoot} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface CachedGsonKeyValueN5Reader extends GsonKeyValueN5Reader {

	boolean cacheMeta();

	default HierarchyStore createHierarchyStore(
			final KeyValueRoot keyValueRoot,
			final boolean cacheMeta) {

		final KeyValueRootHierarchyStore store = new KeyValueRootHierarchyStore(keyValueRoot);
		return cacheMeta ? new HierarchyCache(store) : store;
	}
}
