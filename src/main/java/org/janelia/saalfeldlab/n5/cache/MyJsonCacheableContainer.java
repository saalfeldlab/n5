package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.GsonN5Reader;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5Reader;

public interface MyJsonCacheableContainer extends DelegateStore {

	/**
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code group}.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @return the attributes as a json element, or {@code null} if the attributes file does not exist
	 *
	 * @throws N5IOException
	 * 		if there is an error reading the attributes file
	 * @see GsonN5Reader#getAttributes
	 */
	default JsonElement my_getAttributesFromContainer(N5GroupPath group, String attributesKey) throws N5IOException {
		return readAttributesJson(group, attributesKey);
	}

	/**
	 * List all directory-like children (groups and datasets) in the given
	 * {@code group}.
	 * <p>
	 * The returned child paths are normal paths relative to the given {@code
	 * group} (no leading or trailing slashes). To obtain the path of a child
	 * relative to the container root {@link N5GroupPath#resolve group.resolve}
	 * the child path.
	 *
	 * @param group
	 * 		group path
	 *
	 * @return list of children
	 *
	 * @throws N5NoSuchKeyException
	 * 		if the given path does not exist
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 * @see N5Reader#list
	 */
	String[] my_listFromContainer(N5GroupPath group) throws N5IOException;

	/**
	 * Query whether a directory exists in this container.
	 *
	 * @param group
	 * 		group path
	 *
	 * @return true if the directory exists
	 */
	boolean my_isDirectoryFromContainer(N5GroupPath group);

}
