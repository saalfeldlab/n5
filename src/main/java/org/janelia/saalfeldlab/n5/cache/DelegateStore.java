package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path;

public interface DelegateStore {

	/**
	 * Read an attributes tree from the store
	 *
	 * @param group
	 * 		parent of the attributes file
	 * @param filename
	 * 		filename of the attributes file
	 *
	 * @return the attributes
	 *
	 * @throws N5IOException
	 * 		if an error occurs while writing the attributes
	 */
	// TODO: replace (group, filename) with N5FilePath (not sure, but looks like a good idea...)
	JsonElement readAttributesJson(
			final N5Path.N5GroupPath group,
			final String filename) throws N5IOException;

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
	void writeAttributesJson(
			final N5Path.N5GroupPath group,
			final String filename,
			final JsonElement attributes) throws N5IOException;

}
