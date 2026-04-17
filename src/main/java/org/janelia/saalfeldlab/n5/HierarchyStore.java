package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;

/**
 * Operations on a directory hierarchy (or directory-like hierarchy like
 * prefixes in a key value store):
 * <ul>
 * <li>creating directories</li>
 * <li>listing directory-like children of a directory</li>
 * <li>querying existence of a directory</li>
 * <li>reading json files as {@code JsonElement}</li>
 * <li>writing {@code JsonElement} into json files</li>
 * </ul>
 */
public interface HierarchyStore {

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ READ:                                                                 │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * Read an attributes tree from the store.
	 * <p>
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code parent}.
	 * <p>
	 * NB: If the {@code JsonElement} is retrieved from a cache, a {@link
	 * JsonElement#deepCopy() deepCopy} is returned. It is therefore safe to
	 * modify. Further defensive copying is not necessary.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @param parent
	 * 		directory containing the attributes file
	 * @param filename
	 * 		filename of the attributes file
	 *
	 * @return the attributes as a json element, or {@code null} if the attributes file does not exist
	 *
	 * @throws N5IOException
	 * 		if an error occurs while reading the attributes
	 * @see GsonN5Reader#getAttributes
	 */
	JsonElement readAttributesJson(
			N5DirectoryPath parent,
			String filename,
			Gson gson) throws N5IOException;

	/**
	 * Query whether a directory exists in this container.
	 *
	 * @param path
	 * 		directory path
	 *
	 * @return true if the directory exists
	 */
	boolean isDirectory(N5DirectoryPath path);

	/**
	 * List all directory-like children (groups and datasets) in the given
	 * directory {@code path}.
	 * <p>
	 * The returned child paths are normal paths relative to the given directory
	 * {@code path} (no leading or trailing slashes). To obtain the path of a
	 * child relative to the container root, {@link N5DirectoryPath#resolve
	 * path.resolve} the child path.
	 *
	 * @param path
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
	String[] listDirectories(N5DirectoryPath path) throws N5IOException;


	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ WRITE:                                                                │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * Write an attributes tree into the store
	 *
	 * @param parent
	 * 		directory containing the attributes file
	 * @param filename
	 * 		filename of the attributes file
	 * @param attributes
	 * 		to write
	 *
	 * @throws N5IOException
	 * 		if an error occurs while writing the attributes
	 */
	void writeAttributesJson(
			N5DirectoryPath parent,
			String filename,
			JsonElement attributes,
			Gson gson) throws N5IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param path
	 * 		directory path to create, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during creation
	 */
	void createDirectories(N5DirectoryPath path) throws N5IOException;

	/**
	 * Delete a directory, recursively.
	 *
	 * @param path
	 * 		path to delete, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during deletion
	 */
	void removeDirectory(N5DirectoryPath path) throws N5IOException;

}
