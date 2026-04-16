package org.janelia.saalfeldlab.n5;

import com.google.gson.JsonElement;
import java.lang.reflect.Type;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5JsonParseException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;


/**
 * Methods to read/write groups, datasets, and attributes of a container. This
 * abstracts how attributes are stored, what constitutes a group or a dataset,
 * etc, because these details vary between N5, Zarr v2, and Zarr v3.
 * <p>
 * {@code ContainerDialect} is used by {@link GsonKeyValueN5Reader} and {@link
 * GsonKeyValueN5Writer} for hierarchy and metadata management (basically
 * everything that does not concern {@code DataBlock}s).
 */
// TODO Should this be generically typed on the DatasetAttributes sub-class?
public interface ContainerDialect {

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ READ:                                                                 │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * Read an attribute of a group or dataset.
	 *
	 * @param path
	 * 		group or dataset path
	 * @param attributePath
	 * 		attribute path
	 * @param type
	 * 		attribute Type (use this for specifying generic types)
	 * @param <T>
	 * 		the attribute type
	 *
	 * @return the attribute or {@code null} if the path or attribute does not exist
	 *
	 * @throws N5IOException
	 * 		if an IO error occurs while reading the attributes file
	 * @throws N5ClassCastException
	 * 		if the attribute exists but is not of the specified {@code type}
	 */
	<T> T getAttribute(
			N5DirectoryPath path,
			String attributePath,
			Type type) throws N5IOException, N5ClassCastException;

	/**
	 * Get mandatory attributes (for this dialect) of a dataset.
	 *
	 * @param path
	 * 		dataset path
	 *
	 * @return dataset attributes or {@code null} if the given path is not a dataset
	 *
	 * @throws N5IOException
	 * 		if an IO error occurs while reading the attributes file
	 */
	DatasetAttributes getDatasetAttributes(N5DirectoryPath path) throws N5IOException;

	/**
	 * Test whether a dataset exists at a given path.
	 *
	 * @param path
	 * 		path to check
	 *
	 * @return {@code true} if path is a dataset
	 *
	 * @throws N5IOException
	 * 		if an error occurs (other than the path not existing)
	 */
	boolean datasetExists(N5DirectoryPath path) throws N5IOException;

	/**
	 * Test whether a group exists at a given path.
	 *
	 * @param path
	 * 		path to check
	 *
	 * @return {@code true} if path is a group
	 *
	 * @throws N5IOException
	 * 		if an error occurs (other than the path not existing)
	 */
	boolean groupExists(N5DirectoryPath path) throws N5IOException;

	/**
	 * List all groups and datasets in a group.
	 * <p>
	 * Implementations are typically not strict on correctness: They may not
	 * check whether the requested {@code group} is actually a group. They may
	 * list all "directory-like" children of {@code group}, and not verify that
	 * they are groups.
	 * <p>
	 * The returned child paths are normal paths relative to the given {@code
	 * group} (no leading or trailing slashes). To obtain the path of a child
	 * relative to the container root {@link N5DirectoryPath#resolve group.resolve}
	 * the child path.
	 *
	 * @param path
	 * 		path to list
	 *
	 * @return list of children
	 *
	 * @throws N5NoSuchKeyException
	 * 		if the given group does not exist (or is not a group)
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 */
	String[] list(N5DirectoryPath path) throws N5IOException;

	/**
	 * List all attributes and their class of the group or dataset at the given {@code path}.
	 *
	 * @param path
	 * 		dataset or group path
	 *
	 * @return a map of attribute keys to their inferred class
	 *
	 * @throws N5IOException
	 * 		if an error occurs retrieving the attributes
	 * @throws N5JsonParseException
	 * 		if an error occurs parsing the attributes
	 */
	Map<String, Class<?>> listAttributes(N5DirectoryPath path) throws N5IOException, N5JsonParseException;

	/**
	 * Reads the attributes of a group or dataset.
	 *
	 * @param path
	 * 		dataset or group path
	 * @return the attributes
	 * @throws N5IOException if the attributes could not be read
	 */
	JsonElement getAttributes(N5DirectoryPath path) throws N5IOException;

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ WRITE:                                                                │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * Sets an attribute.
	 *
	 * @param path group or dataset path
	 * @param attributePath the key
	 * @param attribute the attribute
	 * @param <T> the attribute type
	 *
	 * TODO: exceptions...
	 */
	<T> void setAttribute(
			N5DirectoryPath path,
			String attributePath,
			T attribute) throws N5IOException;

	/**
	 * Sets a map of attributes. The passed attributes are inserted into the
	 * existing attribute tree. New attributes, including their parent objects
	 * will be added, existing attributes whose paths are not included will
	 * remain unchanged, those whose paths are included will be overridden.
	 *
	 * @param path group or dataset path
	 * @param attributes the attribute map of attribute paths and values
	 *
	 * TODO: exceptions...
	 */
	void setAttributes(
			N5DirectoryPath path,
			Map<String, ?> attributes) throws N5IOException;

	/**
	 * Remove the attribute with the given {@code attributePath} from the
	 * dataset or group at {@code path}.
	 *
	 * @param path group or dataset path
	 * @param attributePath of attribute to remove
	 * @return true if attribute removed, else false
	 * @throws N5IOException TODO
	 */
	boolean removeAttribute(
			N5DirectoryPath path,
			String attributePath) throws N5IOException;


	/**
	 * Remove and return the attribute of type {@code T} with the given {@code
	 * attributePath} from the dataset or group at {@code path}.
	 * <p>
	 * If an attribute with the given {@code attributePath} exists, but is not
	 * of type {@code T}, it is not removed.
	 *
	 * @param path group or dataset path
	 * @param attributePath of attribute to remove
	 * @param clazz of the attribute to remove
	 * @param <T> of the attribute
	 * @return the removed attribute, as {@code T}, or {@code null} if no
	 *         matching attribute
	 * @throws N5Exception if removing the attribute failed, parsing the attribute failed, or the attribute cannot be interpreted as T
	 */
	<T> T removeAttribute(
			N5DirectoryPath path,
			String attributePath,
			Class<T> clazz) throws N5Exception; // TODO: exception types

	/**
	 * Sets mandatory dataset attributes of the dataset at the given {@code path}.
	 * Creates the dataset if it does not exist.
	 *
	 * @param path dataset path
	 * @param attributes the dataset attributes
	 * @throws N5IOException TODO
	 */
	void setDatasetAttributes(
			N5DirectoryPath path,
			DatasetAttributes attributes) throws N5IOException;

	/**
	 * Creates a group at the given {@code path}.
	 *
	 * @param path the path
	 * @throws N5IOException TODO
	 */
	void createGroup(N5DirectoryPath path) throws N5IOException;

	/**
	 * Creates a dataset. This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param path dataset path
	 * @param attributes the dataset attributes
	 * @throws N5IOException TODO
	 */
	void createDataset(
			N5DirectoryPath path,
			DatasetAttributes attributes) throws N5IOException;


	/**
	 * Removes a group or dataset (directory and all contained files).
	 *
	 * @param path the path to remove
	 * @return true if removal was successful, false otherwise
	 * @throws N5IOException TODO
	 */
	boolean remove(N5DirectoryPath path) throws N5IOException;

}
