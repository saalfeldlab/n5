package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.GsonN5Reader;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.N5Reader;

public interface DelegateStore {

	// ------------------------------------------------------------------------
	//
	// -- DelegateStore : READ --
	//

	/**
	 * Read an attributes tree from the store.
	 * <p>
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code group}.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @param group
	 * 		parent of the attributes file
	 * @param filename
	 * 		filename of the attributes file
	 *
	 * @return the attributes as a json element, or {@code null} if the attributes file does not exist
	 *
	 * @throws N5IOException
	 * 		if an error occurs while reading the attributes
	 * @see GsonN5Reader#getAttributes
	 */
	// TODO: replace (group, filename) with N5FilePath (not sure, but looks like a good idea...)
	JsonElement store_readAttributesJson(
			N5GroupPath group,
			String filename,
			Gson gson) throws N5IOException;

	/**
	 * Query whether a directory exists in this container.
	 *
	 * @param group
	 * 		group path
	 *
	 * @return true if the directory exists
	 */
	// TODO rename
	boolean store_isDirectory(N5GroupPath group);

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
	 * @throws N5Exception.N5NoSuchKeyException
	 * 		if the given path does not exist
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 * @see N5Reader#list
	 */
	String[] store_listDirectories(N5GroupPath group) throws N5IOException;


	// ------------------------------------------------------------------------
	//
	// -- DelegateStore : WRITE --
	//

	/**
	 * Write an attributes tree into the store
	 *
	 * @param group
	 * 		parent of the attributes file
	 * @param filename
	 * 		filename of the attributes file
	 * @param attributes
	 * 		to write
	 *
	 * @throws N5IOException
	 * 		if an error occurs while writing the attributes
	 */
	// TODO: replace (group, filename) with N5FilePath (not sure, but looks like a good idea...)
	void store_writeAttributesJson(
			N5GroupPath group,
			String filename,
			JsonElement attributes,
			Gson gson) throws N5IOException;

	void store_removeAttributesJson(
			final N5GroupPath group,
			final String filename) throws N5IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param group
	 * 		directory path to create, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during creation
	 */
	void store_createDirectories(N5GroupPath group) throws N5IOException;

	/**
	 * Delete a directory, recursively.
	 *
	 * @param group
	 * 		path to delete, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during deletion
	 */
	void store_removeDirectory(N5GroupPath group) throws N5IOException;

}
