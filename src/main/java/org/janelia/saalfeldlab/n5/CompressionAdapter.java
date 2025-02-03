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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.janelia.saalfeldlab.n5.Compression.CompressionParameter;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.scijava.annotations.Index;
import org.scijava.annotations.IndexItem;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Compression adapter, auto-discovers annotated compression implementations
 * in the classpath.
 *
 * @author Stephan Saalfeld
 */
public class CompressionAdapter implements JsonDeserializer<Compression>, JsonSerializer<Compression> {

	private static CompressionAdapter instance = null;

	private final HashMap<String, Constructor<? extends Compression>> compressionConstructors = new HashMap<>();
	private final HashMap<String, HashMap<String, Class<?>>> compressionParameters = new HashMap<>();

	private static ArrayList<Field> getDeclaredFields(Class<?> clazz) {

		final ArrayList<Field> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
		for (clazz = clazz.getSuperclass(); clazz != null; clazz = clazz.getSuperclass())
			fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
		return fields;
	}

	@SuppressWarnings("unchecked")
	public static synchronized void update(final boolean override) {

		if (override || instance == null) {

			final CompressionAdapter newInstance = new CompressionAdapter();

			final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			final Index<CompressionType> annotationIndex = Index.load(CompressionType.class, classLoader);
			for (final IndexItem<CompressionType> item : annotationIndex) {
				System.out.println("item.className() = " + item.className());
			}
			for (final IndexItem<CompressionType> item : annotationIndex) {
				Class<? extends Compression> clazz;
				try {
					clazz = (Class<? extends Compression>)Class.forName(item.className());
					final String type = clazz.getAnnotation(CompressionType.class).value();

					final Constructor<? extends Compression> constructor = clazz.getDeclaredConstructor();

					final HashMap<String, Class<?>> parameters = new HashMap<>();
					final ArrayList<Field> fields = getDeclaredFields(clazz);
					for (final Field field : fields) {
						if (field.getAnnotation(CompressionParameter.class) != null) {
							parameters.put(field.getName(), field.getType());
						}
					}

					newInstance.compressionConstructors.put(type, constructor);
					newInstance.compressionParameters.put(type, parameters);
				} catch (final ClassNotFoundException | NoSuchMethodException | ClassCastException
						| UnsatisfiedLinkError e) {
					System.err.println("Compression '" + item.className() + "' could not be registered");
				}
			}

			instance = newInstance;
		}
	}

	public static void update() {

		update(false);
	}

	@Override
	public JsonElement serialize(
			final Compression compression,
			final Type typeOfSrc,
			final JsonSerializationContext context) {

		final String type = compression.getType();
		final Class<? extends Compression> clazz = compression.getClass();

		final JsonObject json = new JsonObject();
		json.addProperty("type", type);

		final HashMap<String, Class<?>> parameterTypes = compressionParameters.get(type);
		try {
			for (final Entry<String, Class<?>> parameterType : parameterTypes.entrySet()) {
				final String name = parameterType.getKey();
				final Field field = clazz.getDeclaredField(name);
				final boolean isAccessible = field.isAccessible();
				field.setAccessible(true);
				final Object value = field.get(compression);
				field.setAccessible(isAccessible);
				json.add(parameterType.getKey(), context.serialize(value));
			}
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace(System.err);
			return null;
		}

		return json;
	}

	@Override
	public Compression deserialize(
			final JsonElement json,
			final Type typeOfT,
			final JsonDeserializationContext context) throws JsonParseException {

		final JsonObject jsonObject = json.getAsJsonObject();
		final JsonElement jsonType = jsonObject.get("type");
		if (jsonType == null)
			return null;

		final String type = jsonType.getAsString();
		final Constructor<? extends Compression> constructor = compressionConstructors.get(type);
		final Compression compression;
		try {
			compression = constructor.newInstance();
			final HashMap<String, Class<?>> parameterTypes = compressionParameters.get(type);
			for (final Entry<String, Class<?>> parameterType : parameterTypes.entrySet()) {
				final String name = parameterType.getKey();
				if (jsonObject.has(name)) {
					final Object parameter = context.deserialize(jsonObject.get(name), parameterType.getValue());
					ReflectionUtils.setFieldValue(compression, name, parameter);
				}
			}
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| SecurityException | NoSuchFieldException e) {
			e.printStackTrace(System.err);
			return null;
		}

		return compression;
	}

	public static CompressionAdapter getJsonAdapter() {

		if (instance == null)
			update();
		return instance;
	}
}