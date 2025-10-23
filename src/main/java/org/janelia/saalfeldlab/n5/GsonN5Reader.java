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

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

/**
 * {@link N5Reader} with JSON attributes parsed with {@link Gson}.
 *
 */
public interface GsonN5Reader extends N5Reader {

	Gson getGson();

	/**
	 * Get the key for the {@link KeyValueAccess}, that is used for storing attributes.
	 *
	 * @return the attributes key
	 */
	String getAttributesKey();

	@Override
	default Map<String, Class<?>> listAttributes(final String pathName) throws N5Exception {

		return GsonUtils.listAttributes(getAttributes(pathName));
	}

	@Override
	default DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		final JsonElement attributes = getAttributes(normalPath);
		return createDatasetAttributes(attributes);
	}

	default DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

		try {
			final long[] dimensions = GsonUtils.readAttribute(attributes, DatasetAttributes.DIMENSIONS_KEY, long[].class, getGson());
			if (dimensions == null) {
				return null;
			}

			final DataType dataType = GsonUtils.readAttribute(attributes, DatasetAttributes.DATA_TYPE_KEY, DataType.class, getGson());
			if (dataType == null) {
				return null;
			}

			final int[] blockSize = GsonUtils.readAttribute(attributes, DatasetAttributes.BLOCK_SIZE_KEY, int[].class, getGson());
			final Compression compression = GsonUtils.readAttribute(attributes, DatasetAttributes.COMPRESSION_KEY, Compression.class, getGson());

			/* version 0 */
			final String compressionVersion0Name = compression == null
					? GsonUtils.readAttribute(attributes, DatasetAttributes.compressionTypeKey, String.class, getGson())
					: null;

			return DatasetAttributes.from(dimensions, dataType, blockSize, compression, compressionVersion0Name);
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			/* We cannot create a dataset, so return null. */
			return null;
		}
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws N5Exception {

		final String normalPathName = N5URI.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);

		final JsonElement attributes = getAttributes(normalPathName);
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	@Override
	default <T> T getAttribute(final String pathName, final String key, final Type type) throws N5Exception {

		final String normalPathName = N5URI.normalizeGroupPath(pathName);
		final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
		final JsonElement attributes = getAttributes(normalPathName);
		try {
			return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, getGson());
		} catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
			throw new N5Exception.N5ClassCastException(e);
		}
	}

	/**
	 * Reads or the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	JsonElement getAttributes(final String pathName) throws N5Exception;
}
