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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract base class implementing {@link N5Reader} with JSON attributes
 * parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public abstract class AbstractGsonReader implements GsonAttributesParser, N5Reader {

	private static final Pattern ARRAY_INDEX = Pattern.compile("\\[([0-9]+)]");

	protected final Gson gson;

	/**
	 * Constructs an {@link AbstractGsonReader} with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param gsonBuilder
	 */
	public AbstractGsonReader(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(Compression.class, CompressionAdapter.getJsonAdapter());
		gsonBuilder.disableHtmlEscaping();
		this.gson = gsonBuilder.create();
	}

	/**
	 * Constructs an {@link AbstractGsonReader} with a default
	 * {@link GsonBuilder}.
	 */
	public AbstractGsonReader() {

		this(new GsonBuilder());
	}

	@Override
	public Gson getGson() {

		return gson;
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		final Gson gson = getGson();

		final long[] dimensions = GsonAttributesParser.parseAttribute(map, DatasetAttributes.dimensionsKey, long[].class, gson);
		if (dimensions == null)
			return null;

		final DataType dataType = GsonAttributesParser.parseAttribute(map, DatasetAttributes.dataTypeKey, DataType.class, gson);
		if (dataType == null)
			return null;

		int[] blockSize = GsonAttributesParser.parseAttribute(map, DatasetAttributes.blockSizeKey, int[].class, gson);
		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		Compression compression = GsonAttributesParser.parseAttribute(map, DatasetAttributes.compressionKey, Compression.class, gson);

		/* version 0 */
		if (compression == null) {
			switch (GsonAttributesParser.parseAttribute(map, DatasetAttributes.compressionTypeKey, String.class, gson)) {
			case "raw":
				compression = new RawCompression();
				break;
			case "gzip":
				compression = new GzipCompression();
				break;
			case "bzip2":
				compression = new Bzip2Compression();
				break;
			case "lz4":
				compression = new Lz4Compression();
				break;
			case "xz":
				compression = new XzCompression();
				break;
			}
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression);
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		return GsonAttributesParser.parseAttribute(map, key, clazz, getGson());
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		return GsonAttributesParser.parseAttribute(map, key, type, getGson());
	}

	@Override
	public <T> T getAttribute(
			final N5URL url,
			final Type type) throws IOException {

		final Class<?> clazz = (type instanceof Class<?>) ? ((Class<?>)type) : null;
		Type mapType = new TypeToken<Map<String, Object>>() {

		}.getType();
		TypeAdapter<JsonElement> jsonElementTypeAdapter = gson.getAdapter(JsonElement.class);
		HashMap<String, JsonElement> map = getAttributes(url.resolveDataset());

		final String attributePath = url.resolveAttribute();
		JsonElement json = null;
		for (final String pathPart : attributePath.split("/")) {
			if (pathPart.isEmpty())
				continue;
			if (map.containsKey(pathPart)) {
				json = map.get(pathPart);
			} else if (json instanceof JsonObject && json.getAsJsonObject().get(pathPart) != null) {
				json = json.getAsJsonObject().get(pathPart);
			} else {
				final Matcher matcher = ARRAY_INDEX.matcher(pathPart);
				if (json != null && json.isJsonArray() && matcher.matches()) {
					final int index = Integer.parseInt(matcher.group().replace("[", "").replace("]", ""));
					json = json.getAsJsonArray().get(index);
				}
			}
		}
		if (json == null && clazz != null && clazz.isAssignableFrom(HashMap.class)) {
			//NOTE: Would not need to do this if we could get the root `JsonElement` attribute, instead of just the `Map<String, JsonElement`
			/* reconstruct the tree to parse as Object */
			final PipedWriter writer = new PipedWriter();
			//TODO: writer blocks if there is not enough space for new content. What do we want to do here?
			final PipedReader in = new PipedReader(writer, 1000);

			final JsonWriter out = new JsonWriter(writer);
			out.beginObject();
			for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
				String k = entry.getKey();
				JsonElement val = entry.getValue();
				out.name(k);
				jsonElementTypeAdapter.write(out, val);
			}
			out.endObject();

			Map<String, Object> retMap = gson.fromJson(new JsonReader(in), mapType);
			return (T)retMap;
		}
		if (json instanceof JsonArray) {
			final JsonArray array = json.getAsJsonArray();
			T retArray = getJsonAsArray(array, type);
			if (retArray != null)
				return retArray;
		}
		try {
			return gson.fromJson(json, type);
		} catch (
				JsonSyntaxException e) {
			return null;
		}
	}

	@Override
	public <T> T getAttribute(
			final N5URL url,
			final Class<T> clazz) throws IOException {

		return getAttribute(url, (Type)clazz);
	}

	private <T> T getJsonAsArray(JsonArray array, Class<T> clazz) {
		return getJsonAsArray(array, (Type)clazz);
	}

	private <T> T getJsonAsArray(JsonArray array, Type type) {


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
		} else if(clazz != null && clazz.isArray()) {
			final Class<?> componentCls = clazz.getComponentType();
			final Object[] clsArray = (Object[] )Array.newInstance(componentCls, array.size());
			for (int i = 0; i < array.size(); i++) {
				clsArray[i] = gson.fromJson(array.get(i), componentCls);
			}
			return (T)clsArray;
		}
		return null;
	}
}
