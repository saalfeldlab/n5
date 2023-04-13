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
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;

/**
 * {@link N5Reader} for JSON attributes parsed by {@link Gson}.
 *
 * @author Stephan Saalfeld
 */
@Deprecated
public interface GsonAttributesParser extends N5Reader {

	@Deprecated
	public Gson getGson();

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return the attributes
	 * @throws IOException the exception
	 */
	@Deprecated
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException;

	/**
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map the attributes map
	 * @param key the key
	 * @param clazz the class
	 * @param gson the gson
	 * @param <T> the attribute type
	 * @return the attribute
	 * @throws IOException the exception
	 */
	@Deprecated
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
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map the attributes map
	 * @param key the key
	 * @param type the type 
	 * @param gson the gson
	 * @param <T> the attribute type
	 * @return the attribute
	 * @throws IOException the exception
	 */
	@Deprecated
	public static <T> T parseAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Type type,
			final Gson gson) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, type);
		else
			return null;
	}

	/**
	 * Reads the attributes map from a given {@link Reader}.
	 *
	 * @param reader the reader
	 * @param gson the gson
	 * @return the attributes map
	 * @throws IOException the exception
	 */
	@Deprecated
	public static HashMap<String, JsonElement> readAttributes(final Reader reader, final Gson gson) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		final HashMap<String, JsonElement> map = gson.fromJson(reader, mapType);
		return map == null ? new HashMap<>() : map;
	}

	/**
	 * Inserts new the JSON export of attributes into the given attributes map.
	 *
	 * @param map the target attributes map to be edited
	 * @param attributes the source attributes map
	 * @param gson the gson
	 * @throws IOException the exception
	 */
	@Deprecated
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
	 * @param writer the writer
	 * @param map the attributes map
	 * @param gson the gson
	 * @throws IOException the exception
	 */
	@Deprecated
	public static void writeAttributes(
			final Writer writer,
			final HashMap<String, JsonElement> map,
			final Gson gson) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		gson.toJson(map, mapType, writer);
		writer.flush();
	}

	/**
	 * Return a reasonable class for a {@link JsonPrimitive}.  Possible return
	 * types are
	 * <ul>
	 * <li>boolean</li>
	 * <li>double</li>
	 * <li>String</li>
	 * <li>Object</li>
	 * </ul>
	 *
	 * @param jsonPrimitive the json
	 * @return the class
	 */
	@Deprecated
	public static Class<?> classForJsonPrimitive(final JsonPrimitive jsonPrimitive) {

		if (jsonPrimitive.isBoolean())
			return boolean.class;
		else if (jsonPrimitive.isNumber()) {
			final Number number = jsonPrimitive.getAsNumber();
			if (number.longValue() == number.doubleValue())
				return long.class;
			else
				return double.class;
		} else if (jsonPrimitive.isString())
			return String.class;
		else return Object.class;
	}

	/**
	 * Best effort implementation of {@link N5Reader#listAttributes(String)}
	 * with limited type resolution.  Possible return types are
	 * <ul>
	 * <li>null</li>
	 * <li>boolean</li>
	 * <li>double</li>
	 * <li>String</li>
	 * <li>Object</li>
	 * <li>boolean[]</li>
	 * <li>double[]</li>
	 * <li>String[]</li>
	 * <li>Object[]</li>
	 * </ul>
	 * 
	 * @param pathName the path
	 * @return the attributes map
	 */
	@Override
	@Deprecated
	public default Map<String, Class<?>> listAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> jsonElementMap = getAttributes(pathName);
		final HashMap<String, Class<?>> attributes = new HashMap<>();
		jsonElementMap.forEach(
				(key, jsonElement) -> {
					final Class<?> clazz;
					if (jsonElement.isJsonNull())
						clazz = null;
					else if (jsonElement.isJsonPrimitive())
						clazz = classForJsonPrimitive((JsonPrimitive)jsonElement);
					else if (jsonElement.isJsonArray()) {
						final JsonArray jsonArray = (JsonArray)jsonElement;
						Class<?> arrayElementClass = Object.class;
						if (jsonArray.size() > 0) {
							final JsonElement firstElement = jsonArray.get(0);
							if (firstElement.isJsonPrimitive()) {
								arrayElementClass = classForJsonPrimitive(firstElement.getAsJsonPrimitive());
								for (int i = 1; i < jsonArray.size() && arrayElementClass != Object.class; ++i) {
									final JsonElement element = jsonArray.get(i);
									if (element.isJsonPrimitive()) {
										final Class<?> nextArrayElementClass = classForJsonPrimitive(element.getAsJsonPrimitive());
										if (nextArrayElementClass != arrayElementClass)
											if (nextArrayElementClass == double.class && arrayElementClass == long.class)
												arrayElementClass = double.class;
											else {
												arrayElementClass = Object.class;
												break;
											}
									} else {
										arrayElementClass = Object.class;
										break;
									}
								}
							}
							clazz = Array.newInstance(arrayElementClass, 0).getClass();
						} else
							clazz = Object[].class;
					}
					else
						clazz = Object.class;
					attributes.put(key, clazz);
				});
		return attributes;
	}
}
