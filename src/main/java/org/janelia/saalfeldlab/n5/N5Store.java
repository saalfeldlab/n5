package org.janelia.saalfeldlab.n5;

import com.google.gson.JsonElement;
import java.lang.reflect.Type;
import java.util.Map;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5JsonParseException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;


// TODO Should this be generically typed on the DatasetAttributes sub-class?
public interface N5Store {

	// TODO: think more about which exceptions types should be thrown

	// ┌───────────────────────────────────────────────────────────────────────┐
	// │ READ:                                                                 │
	// └───────────────────────────────────────────────────────────────────────┘

	/**
	 * TODO javadoc
	 *
	 * @param path
	 * @param attributePath
	 * @param type
	 * @return
	 * @param <T>
	 * @throws N5IOException
	 * @throws N5ClassCastException
	 */
	<T> T getAttribute(
			N5GroupPath path,
			String attributePath,
			Type type) throws N5IOException, N5ClassCastException;

	/**
	 * TODO javadoc
	 *
	 * @param path
	 * @return
	 * @throws N5IOException
	 */
	DatasetAttributes getDatasetAttributes(N5GroupPath path) throws N5IOException;

	/**
	 * TODO javadoc
	 *
	 * @param path
	 * @return
	 * @throws N5IOException
	 * 		if an error occurs (other than the path not existing)
	 */
	boolean datasetExists(N5GroupPath path) throws N5IOException;

	/**
	 * TODO javadoc
	 *
	 * @param path
	 * @return
	 * @throws N5IOException
	 * 		if an error occurs (other than the path not existing)
	 */
	boolean groupExists(N5GroupPath path) throws N5IOException;

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
	 * relative to the container root {@link N5GroupPath#resolve group.resolve}
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
	String[] list(N5GroupPath path) throws N5IOException;

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
	Map<String, Class<?>> listAttributes(N5GroupPath path) throws N5IOException, N5JsonParseException;

	/**
	 * Reads the attributes of a group or dataset.
	 *
	 * @param path
	 * 		dataset or group path
	 * @return the attributes
	 * @throws N5IOException if the attributes could not be read
	 */
	JsonElement getAttributes(N5GroupPath path) throws N5IOException;

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
			N5GroupPath path,
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
			N5GroupPath path,
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
			N5GroupPath path,
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
			N5GroupPath path,
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
			N5GroupPath path,
			DatasetAttributes attributes) throws N5IOException;

	/**
	 * Creates a group at the given {@code path}.
	 *
	 * @param path the path
	 * @throws N5IOException TODO
	 */
	void createGroup(N5GroupPath path) throws N5IOException;

	/**
	 * Creates a dataset. This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param path dataset path
	 * @param attributes the dataset attributes
	 * @throws N5IOException TODO
	 */
	void createDataset(
			N5GroupPath path,
			DatasetAttributes attributes) throws N5IOException;


	/**
	 * Removes a group or dataset (directory and all contained files).
	 *
	 * @param path the path to remove
	 * @return true if removal was successful, false otherwise
	 * @throws N5IOException TODO
	 */
	boolean remove(N5GroupPath path) throws N5IOException;

	// TODO: flesh out interface
	//       implement for N5
	//       does any additional caching make sense?
}
