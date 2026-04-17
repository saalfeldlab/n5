/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * {@link N5Reader} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Reader extends N5Reader {

	@Deprecated
	Gson getGson();

	/**
	 * Get the key for the that is used for storing attributes. The N5 format
	 * uses "attributes.json".
	 *
	 * @return the attributes key
	 */
	@Deprecated
	String getAttributesKey();

	// TODO: There is nothing tying the default implementations of the metadata
	//   methods to Gson. They could be moved up to N5Reader. However, we don't
	//   want to put getContainerDialect() there, because we want to avoid
	//   adding new methods to the top-level interfaces.

	ContainerDialect getContainerDialect();

	/**
	 * Reads or the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	@Deprecated
	default JsonElement getAttributes(String pathName) throws N5Exception {

		return getContainerDialect().getAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		return getContainerDialect().getAttribute(N5Path.N5DirectoryPath.of(pathName), key, type);
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().getDatasetAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return getContainerDialect().listAttributes(N5Path.N5DirectoryPath.of(pathName));
	}

	default boolean groupExists(final String pathName) {

		return getContainerDialect().groupExists(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default boolean exists(final String pathName) {

		return groupExists(pathName) || datasetExists(pathName);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		return getContainerDialect().datasetExists(N5Path.N5DirectoryPath.of(pathName));
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getContainerDialect().list(N5Path.N5DirectoryPath.of(pathName));
	}
}
