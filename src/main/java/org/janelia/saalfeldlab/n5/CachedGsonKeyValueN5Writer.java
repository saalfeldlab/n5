package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;

/**
 * Cached default implementation of {@link N5Writer} with JSON attributes parsed
 * with {@link Gson}.
 */
public interface CachedGsonKeyValueN5Writer extends CachedGsonKeyValueN5Reader, GsonKeyValueN5Writer {

}
