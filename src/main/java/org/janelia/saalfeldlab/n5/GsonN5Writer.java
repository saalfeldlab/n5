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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * {@link N5Reader} for JSON attributes parsed by {@link Gson}.
 *
 * @author Stephan Saalfeld
 */
public interface GsonN5Writer extends GsonN5Reader, N5Writer {


	/**
	 * If there is an attribute in {@code root} such that it can be parsed and desrialized as {@link T},
	 * then remove it from {@code root}, write {@code root} to the {@code writer}, and return the removed attribute.
	 * <p>
	 * If there is an attribute at the location specified by {@code normalizedAttributePath} but it cannot be deserialized to {@link T}, then it is not removed.
	 * <p>
	 * If nothing is removed, then {@code root} is not writen to the {@code writer}.
	 *
	 * @param writer                  to write the modified {@code root} to after removal of the attribute
	 * @param root                    to remove the attribute from
	 * @param normalizedAttributePath to the attribute location
	 * @param cls                     of the attribute to remove
	 * @param gson                    to deserialize the attribute with
	 * @param <T>                     of the removed attribute
	 * @return the removed attribute, or null if nothing removed
	 * @throws IOException
	 */
	static <T> T removeAttribute(
			final Writer writer,
			JsonElement root,
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
	 * If there is an attribute in {@code root} such that it can be parsed and desrialized as {@link T},
	 * then remove it from {@code root} and return the removed attribute.
	 * <p>
	 * If there is an attribute at the location specified by {@code normalizedAttributePath} but it cannot be deserialized to {@link T}, then it is not removed.
	 *
	 * @param root                    to remove the attribute from
	 * @param normalizedAttributePath to the attribute location
	 * @param cls                     of the attribute to remove
	 * @param gson                    to deserialize the attribute with
	 * @param <T>                     of the removed attribute
	 * @return the removed attribute, or null if nothing removed
	 */
	static <T> T removeAttribute(final JsonElement root, final String normalizedAttributePath, final Class<T> cls, final Gson gson) {

		final T attribute = GsonN5Reader.readAttribute(root, normalizedAttributePath, cls, gson);
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
	static JsonElement removeAttribute(JsonElement root, String normalizedAttributePath) {

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
	 * @param writer
	 * @param root
	 * @param normalizedAttributePath
	 * @param attribute
	 * @param gson
	 * @throws IOException
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
	 * @param writer
	 * @param root
	 * @throws IOException
	 */
	static <T> void writeAttributes(
			final Writer writer,
			JsonElement root,
			final Gson gson) throws IOException {

		gson.toJson(root, writer);
		writer.flush();
	}

	static JsonElement insertAttributes(JsonElement root, Map<String, ?> attributes, Gson gson) {

		for (Map.Entry<String, ?> attribute : attributes.entrySet()) {
			root = insertAttribute(root, N5URL.normalizeAttributePath(attribute.getKey()), attribute.getValue(), gson);
		}
		return root;
	}

	static <T> JsonElement insertAttribute(JsonElement root, String normalizedAttributePath, T attribute, Gson gson) {

		LinkedAttributePathToken<?> pathToken = N5URL.getAttributePathTokens(normalizedAttributePath);
		/* No path to traverse or build; just write the value */
		if (pathToken == null)
			return gson.toJsonTree(attribute);

		JsonElement json = root;
		while (pathToken != null) {

			JsonElement parent = pathToken.setAndCreateParentElement(json);

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
