/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
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
import java.util.regex.Matcher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * {@link N5Reader} for JSON attributes parsed by {@link Gson}.
 *
 * @author Stephan Saalfeld
 */
public interface GsonUtils {

	static Gson registerGson(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		return gsonBuilder.create();
	}

	/**
	 * Reads the attributes json from a given {@link Reader}.
	 *
	 * @param reader the reader
	 * @return the root {@link JsonObject} of the attributes
	 * @throws IOException the exception
	 */
	static JsonElement readAttributes(final Reader reader, final Gson gson) throws IOException {

		final JsonElement json = gson.fromJson(reader, JsonElement.class);
		return json;
	}

	static <T> T readAttribute(final JsonElement root, final String normalizedAttributePath, final Class<T> cls, final Gson gson) {

		return readAttribute(root, normalizedAttributePath, TypeToken.get(cls).getType(), gson);
	}

	static <T> T readAttribute(final JsonElement root, final String normalizedAttributePath, final Type type, final Gson gson) {

		final JsonElement attribute = getAttribute(root, normalizedAttributePath);
		return parseAttributeElement(attribute, gson, type);
	}

	/**
	 * Deserialize the {@code attribute} as {@link Type type} {@code T}.
	 *
	 * @param attribute to deserialize as {@link Type type}
	 * @param gson      used to deserialize {@code attribute}
	 * @param type      to desrialize {@code attribute} as
	 * @param <T>       return type represented by {@link Type type}
	 * @return the deserialized attribute object, or {@code null} if {@code attribute} cannot deserialize to {@code T}
	 */
	static <T> T parseAttributeElement(final JsonElement attribute, final Gson gson, final Type type) {

		if (attribute == null)
			return null;

		final Class<?> clazz = (type instanceof Class<?>) ? ((Class<?>)type) : null;
		if (clazz != null && clazz.isAssignableFrom(HashMap.class)) {
			final Type mapType = new TypeToken<Map<String, Object>>() {

			}.getType();
			final Map<String, Object> retMap = gson.fromJson(attribute, mapType);
			//noinspection unchecked
			return (T)retMap;
		}
		if (attribute instanceof JsonArray) {
			final JsonArray array = attribute.getAsJsonArray();
			try {
				final T retArray = GsonUtils.getJsonAsArray(gson, array, type);
				if (retArray != null)
					return retArray;
			} catch (final JsonSyntaxException e) {
				if (type == String.class)
					return (T)gson.toJson(attribute);
				return null;
			} catch ( final NumberFormatException nfe ) {
				return null;
			}
		}
		try {
			return gson.fromJson(attribute, type);
		} catch (final JsonSyntaxException e) {
			if (type == String.class)
				return (T)gson.toJson(attribute);
			return null;
		} catch ( final NumberFormatException nfe ) {
			return null;
		}
	}

	/**
	 * Return the attribute at {@code normalizedAttributePath} as a
	 * {@link JsonElement}. Does not attempt to parse the attribute.
	 *
	 * @param root                    to search for the {@link JsonElement} at
	 *                                location {@code normalizedAttributePath}
	 * @param normalizedAttributePath to the attribute
	 * @return the attribute as a {@link JsonElement}.
	 */
	static JsonElement getAttribute(JsonElement root, final String normalizedAttributePath) {

		final String[] pathParts = normalizedAttributePath.split("(?<!\\\\)/");
		for (int i = 0; i < pathParts.length; i++) {
			final String pathPart = pathParts[i];
			if (pathPart.isEmpty())
				continue;
			final String pathPartWithoutEscapeCharacters = pathPart
					.replaceAll("\\\\/", "/")
					.replaceAll("\\\\\\[", "[");
			if (root instanceof JsonObject && root.getAsJsonObject().get(pathPartWithoutEscapeCharacters) != null) {
				final JsonObject jsonObject = root.getAsJsonObject();
				root = jsonObject.get(pathPartWithoutEscapeCharacters);
			} else {
				final Matcher matcher = N5URL.ARRAY_INDEX.matcher(pathPart);
				if (root != null && root.isJsonArray() && matcher.matches()) {
					final int index = Integer.parseInt(matcher.group().replace("[", "").replace("]", ""));
					final JsonArray jsonArray = root.getAsJsonArray();
					if (index >= jsonArray.size()) {
						return null;
					}
					root = jsonArray.get(index);
				} else {
					return null;
				}
			}
		}
		return root;
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
	 * @param root the json element
	 * @return the attribute map
	 */
	static Map<String, Class<?>> listAttributes(final JsonElement root) throws N5Exception.N5IOException {

		if (root == null || !root.isJsonObject()) {
			return null;
		}

		final HashMap<String, Class<?>> attributes = new HashMap<>();
		root.getAsJsonObject().entrySet().forEach(entry -> {
			final Class<?> clazz;
			final String key = entry.getKey();
			final JsonElement jsonElement = entry.getValue();
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
			} else
				clazz = Object.class;
			attributes.put(key, clazz);
		});
		return attributes;
	}

	static <T> T getJsonAsArray(final Gson gson, final JsonArray array, final Class<T> cls) {

		return getJsonAsArray(gson, array, TypeToken.get(cls).getType());
	}

	static <T> T getJsonAsArray(final Gson gson, final JsonArray array, final Type type) {

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
	 * @param jsonPrimitive the json primitive
	 * @return the class
	 */
	static Class<?> classForJsonPrimitive(final JsonPrimitive jsonPrimitive) {

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
		else
			return Object.class;
	}

	/**
	 * If there is an attribute in {@code root} such that it can be parsed and deserialized as {@code T},
	 * then remove it from {@code root}, write {@code root} to the {@code writer}, and return the removed attribute.
	 * <p>
	 * If there is an attribute at the location specified by {@code normalizedAttributePath} but it cannot be deserialized to {@code T}, then it is not removed.
	 * <p>
	 * If nothing is removed, then {@code root} is not written to the {@code writer}.
	 *
	 * @param writer                  to write the modified {@code root} to after removal of the attribute
	 * @param root                    to remove the attribute from
	 * @param normalizedAttributePath to the attribute location
	 * @param cls                     of the attribute to remove
	 * @param gson                    to deserialize the attribute with
	 * @param <T>                     of the removed attribute
	 * @return the removed attribute, or null if nothing removed
	 * @throws IOException the exception
	 */
	static <T> T removeAttribute(
			final Writer writer,
			final JsonElement root,
			final String normalizedAttributePath,
			final Class<T> cls,
			final Gson gson) throws IOException {

		final T removed = removeAttribute(root, normalizedAttributePath, cls, gson);
		if (removed != null) {
			writeAttributes(writer, root, gson);
		}
		return removed;
	}

	/**
	 * If there is an attribute in {@code root} at location {@code normalizedAttributePath} then remove it from {@code root}..
	 *
	 * @param writer                  to write the modified {@code root} to after removal of the attribute
	 * @param root                    to remove the attribute from
	 * @param normalizedAttributePath to the attribute location
	 * @param gson                    to deserialize the attribute with
	 * @return if the attribute was removed or not
	 */
	static boolean removeAttribute(
			final Writer writer,
			final JsonElement root,
			final String normalizedAttributePath,
			final Gson gson) throws IOException {

		final JsonElement removed = removeAttribute(root, normalizedAttributePath, JsonElement.class, gson);
		if (removed != null) {
			writeAttributes(writer, root, gson);
			return true;
		}
		return false;
	}

	/**
	 * If there is an attribute in {@code root} such that it can be parsed and desrialized as {@code T},
	 * then remove it from {@code root} and return the removed attribute.
	 * <p>
	 * If there is an attribute at the location specified by {@code normalizedAttributePath} but it cannot be deserialized to {@code T}, then it is not removed.
	 *
	 * @param root                    to remove the attribute from
	 * @param normalizedAttributePath to the attribute location
	 * @param cls                     of the attribute to remove
	 * @param gson                    to deserialize the attribute with
	 * @param <T>                     of the removed attribute
	 * @return the removed attribute, or null if nothing removed
	 */
	static <T> T removeAttribute(final JsonElement root, final String normalizedAttributePath, final Class<T> cls, final Gson gson) {

		final T attribute = GsonUtils.readAttribute(root, normalizedAttributePath, cls, gson);
		if (attribute != null) {
			removeAttribute(root, normalizedAttributePath);
		}
		return attribute;
	}

	/**
	 * Remove and return the attribute at {@code normalizedAttributePath} as a {@link JsonElement}.
	 * Does not attempt to parse the attribute.
	 *
	 * @param root                    to search for the {@link JsonElement} at location {@code normalizedAttributePath}
	 * @param normalizedAttributePath to the attribute
	 * @return the attribute as a {@link JsonElement}.
	 */
	static JsonElement removeAttribute(JsonElement root, final String normalizedAttributePath) {

		final String[] pathParts = normalizedAttributePath.split("(?<!\\\\)/");
		for (int i = 0; i < pathParts.length; i++) {
			final String pathPart = pathParts[i];
			if (pathPart.isEmpty())
				continue;
			final String pathPartWithoutEscapeCharacters = pathPart
					.replaceAll("\\\\/", "/")
					.replaceAll("\\\\\\[", "[");
			if (root instanceof JsonObject && root.getAsJsonObject().get(pathPartWithoutEscapeCharacters) != null) {
				final JsonObject jsonObject = root.getAsJsonObject();
				root = jsonObject.get(pathPartWithoutEscapeCharacters);
				if (i == pathParts.length - 1) {
					jsonObject.remove(pathPartWithoutEscapeCharacters);
				}
			} else {
				final Matcher matcher = N5URL.ARRAY_INDEX.matcher(pathPart);
				if (root != null && root.isJsonArray() && matcher.matches()) {
					final int index = Integer.parseInt(matcher.group().replace("[", "").replace("]", ""));
					final JsonArray jsonArray = root.getAsJsonArray();
					if (index >= jsonArray.size()) {
						return null;
					}
					root = jsonArray.get(index);
					if (i == pathParts.length - 1) {
						jsonArray.remove(index);
					}
				} else {
					return null;
				}
			}
		}
		return root;
	}

	/**
	 * Inserts {@code attribute} into {@code root} at location {@code normalizedAttributePath} and write the resulting {@code root}.
	 * <p>
	 * If {@code root} is not a {@link JsonObject}, then it is overwritten with an object containing {@code "normalizedAttributePath": attribute }
	 *
	 * @param writer the writer
	 * @param root the root json element
	 * @param normalizedAttributePath the attribute path
	 * @param attribute the attribute
	 * @param gson the gson
	 * @param <T> the attribute type
	 * @throws IOException the exception
	 */
	static <T> void writeAttribute(
			final Writer writer,
			JsonElement root,
			final String normalizedAttributePath,
			final T attribute,
			final Gson gson) throws IOException {

		root = insertAttribute(root, normalizedAttributePath, attribute, gson);
		writeAttributes(writer, root, gson);
	}

	/**
	 * Writes the attributes JsonElemnt to a given {@link Writer}.
	 * This will overwrite any existing attributes.
	 *
	 * @param writer the writer
	 * @param root the root json element
	 * @param gson the gson
	 * @param <T> the attribute type
	 * @throws IOException the exception
	 */
	static <T> void writeAttributes(
			final Writer writer,
			final JsonElement root,
			final Gson gson) throws IOException {

		gson.toJson(root, writer);
		writer.flush();
	}

	static JsonElement insertAttributes(JsonElement root, final Map<String, ?> attributes, final Gson gson) {

		for (final Map.Entry<String, ?> attribute : attributes.entrySet()) {
			root = insertAttribute(root, N5URL.normalizeAttributePath(attribute.getKey()), attribute.getValue(), gson);
		}
		return root;
	}

	static <T> JsonElement insertAttribute(JsonElement root, final String normalizedAttributePath, final T attribute, final Gson gson) {

		LinkedAttributePathToken<?> pathToken = N5URL.getAttributePathTokens(normalizedAttributePath);

		/* No path to traverse or build; just return the value */
		if (pathToken == null)
			return gson.toJsonTree(attribute);

		JsonElement json = root;
		while (pathToken != null) {

			final JsonElement parent = pathToken.setAndCreateParentElement(json);

			/* We may need to create or override the existing root if it is non-existent or incompatible. */
			final boolean rootOverriden = json == root && parent != json;
			if (root == null || rootOverriden) {
				root = parent;
			}

			json = pathToken.writeChild(gson, attribute);

			pathToken = pathToken.next();
		}
		return root;
	}
}
