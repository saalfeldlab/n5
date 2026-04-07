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

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;

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

	@Deprecated
	default DatasetAttributes createDatasetAttributes(JsonElement attributes) {

		return getGson().fromJson(attributes, DatasetAttributes.class);
	}

	/**
	 * Reads or the attributes of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attributes identified by pathName
	 * @throws N5Exception if the attribute cannot be returned
	 */
	@Deprecated
	JsonElement getAttributes(String pathName) throws N5Exception;
}
