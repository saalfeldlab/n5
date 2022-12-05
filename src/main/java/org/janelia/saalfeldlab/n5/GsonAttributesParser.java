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

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
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
	 * Reads the attributes a group or dataset.
	 *
	 * @param pathName group path
	 * @return the root {@link JsonElement} of the attributes
	 * @throws IOException
	 */
	public JsonElement getAttributesJson(final String pathName) throws IOException;

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
	 * Parses an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param type
	 * @return
	 * @throws IOException
	 */
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
	 * Reads the attributes json from a given {@link Reader}.
	 *
	 * @param reader
	 * @return the root {@link JsonObject} of the attributes
	 * @throws IOException
	 */
	public static JsonElement readAttributesJson(final Reader reader, final Gson gson) throws IOException {

		final JsonElement json = gson.fromJson(reader, JsonElement.class);
		return json;
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

	public static <T> void writeAttribute(
			final Writer writer,
			JsonElement root,
			final String attributePath,
			final T attribute,
			final Gson gson) throws IOException {


		JsonElement parent = null;
		JsonElement json = root;
		final List<N5URL.N5UrlAttributePathToken> attributePathTokens = N5URL.getAttributePathTokens(gson, attributePath, attribute);
		for (N5URL.N5UrlAttributePathToken token : attributePathTokens) {
			/* The json is incompatible if it needs to be replaced; json == null is NOT incompatible.
			* The two cases are if either the JsonElement is different than expected, or we are a leaf
			* with no parent, in which case we always clear the existing root/json if any exists. */
			if (token.jsonIncompatible(json) || (token instanceof N5URL.N5UrlAttributePathLeaf<?> && parent == null) ) {
				if (parent == null) {
					/* We are replacing the root, so just set the root to null, and continue */
					root = null;
					json = null;
				} else {
					json = token.replaceJsonElement(parent);
				}
			}

			/* If we can dive into this jsonElement for the current token, do so,
			*  Otherwise, create the element */
			if (token.canNavigate(json) && !(token.child instanceof N5URL.N5UrlAttributePathLeaf<?>)) {
				parent = json;
				json = token.navigateJsonElement(json);
				if (token.child instanceof N5URL.N5UrlAttributePathLeaf<?>) {
					token.writeLeaf(json);
				}
			} else {
				parent = json;
				json = token.createJsonElement(json);
				/* If the root is null, we need to set it based on the newly created element */
				if (root == null) {
					if (token instanceof N5URL.N5UrlAttributePathLeaf<?> ) {
						/* if it's a leaf, we are the root*/
						root = json;
					} else {
						/* Otherwise, creating the element will have specified the root, and we can grab it now. */
						assert token.getRoot() != null;
						root = token.getRoot();
						parent = root;
					}
				}
			}
		}
		gson.toJson(root, writer);
		writer.flush();
	}

	static  <T> T getJsonAsArray(Gson gson, JsonArray array, Type type) {

		final Class<?> clazz = (type instanceof Class<?>) ? ((Class<?>)type) : null;

		if (type == boolean[].class) {
			final boolean[] retArray = new boolean[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final Boolean value = gson.fromJson(array.get(i), boolean.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == double[].class) {
			final double[] retArray = new double[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final double value = gson.fromJson(array.get(i), double.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == float[].class) {
			final float[] retArray = new float[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final float value = gson.fromJson(array.get(i), float.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == long[].class) {
			final long[] retArray = new long[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final long value = gson.fromJson(array.get(i), long.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == short[].class) {
			final short[] retArray = new short[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final short value = gson.fromJson(array.get(i), short.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == int[].class) {
			final int[] retArray = new int[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final int value = gson.fromJson(array.get(i), int.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == byte[].class) {
			final byte[] retArray = new byte[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final byte value = gson.fromJson(array.get(i), byte.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (type == char[].class) {
			final char[] retArray = new char[array.size()];
			for (int i = 0; i < array.size(); i++) {
				final char value = gson.fromJson(array.get(i), char.class);
				retArray[i] = value;
			}
			return (T)retArray;
		} else if (clazz != null && clazz.isArray()) {
			final Class<?> componentCls = clazz.getComponentType();
			final Object[] clsArray = (Object[])Array.newInstance(componentCls, array.size());
			for (int i = 0; i < array.size(); i++) {
				clsArray[i] = gson.fromJson(array.get(i), componentCls);
			}
			//noinspection unchecked
			return (T)clsArray;
		}
		return null;
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
	 * @param jsonPrimitive
	 * @return
	 */
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
	 */
	@Override
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
