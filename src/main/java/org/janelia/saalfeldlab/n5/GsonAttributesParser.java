/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.googlecode.gentyref.TypeToken;

/**
 * {@link N5Reader} for JSON attributes parsed by {@link Gson}.
 *
 * @author Stephan Saalfeld
 */
public interface GsonAttributesParser extends N5Reader {

	public Gson getGson();

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException;

	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param clazz
	 * @return
	 * @throws IOException
	 */
	public static <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Class<T> clazz,
			final Gson gson) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	/**
	 * Reads the attributes map from a given {@link Reader}.
	 *
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String, JsonElement> readAttributes(final Reader reader, final Gson gson) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		final HashMap<String, JsonElement> map = gson.fromJson(reader, mapType);
		return map == null ? new HashMap<>() : map;
	}

	/**
	 * Inserts new the JSON export of attributes into the given attributes map.
	 *
	 * @param map
	 * @param attributes
	 * @param gson
	 * @throws IOException
	 */
	public static void insertAttributes(
			final HashMap<String, JsonElement> map,
			final Map<String, ?> attributes,
			final Gson gson) throws IOException {

		for (final Entry<String, ?> entry : attributes.entrySet())
			map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));
	}

	/**
	 * Writes the attributes map to a given {@link Writer}.
	 *
	 * @param writer
	 * @param map
	 * @throws IOException
	 */
	public static void writeAttributes(
			final Writer writer,
			final HashMap<String, JsonElement> map,
			final Gson gson) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		gson.toJson(map, mapType, writer);
		writer.flush();
	}
}
