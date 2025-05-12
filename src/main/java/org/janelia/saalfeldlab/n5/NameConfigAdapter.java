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

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.saalfeldlab.n5.serialization.N5Annotations;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;
import org.scijava.annotations.Index;
import org.scijava.annotations.IndexItem;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * T adapter, auto-discovers annotated T implementations in the classpath.
 *
 * @author Caleb Hulbert
 */
public class NameConfigAdapter<T> implements JsonDeserializer<T>, JsonSerializer<T> {

	private static HashMap<Class<?>, NameConfigAdapter<?>> adapters = new HashMap<>();

	private static <V> void registerAdapter(Class<V> cls) {

		adapters.put(cls, new NameConfigAdapter<>(cls));
		update(adapters.get(cls));
	}
	private final HashMap<String, Constructor<? extends T>> constructors = new HashMap<>();

	private final HashMap<String, HashMap<String, Field>> parameters = new HashMap<>();
	private final HashMap<String, HashMap<String, String>> parameterNames = new HashMap<>();
	private static ArrayList<Field> getDeclaredFields(Class<?> clazz) {

		final ArrayList<Field> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
		for (clazz = clazz.getSuperclass(); clazz != null; clazz = clazz.getSuperclass())
			fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
		return fields;
	}

	@SuppressWarnings("unchecked")
	public static synchronized <T> void update(final NameConfigAdapter<T> adapter) {

		final String prefix = adapter.type.getAnnotation(NameConfig.Prefix.class).value();
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final Index<NameConfig.Name> annotationIndex = Index.load(NameConfig.Name.class, classLoader);
		for (final IndexItem<NameConfig.Name> item : annotationIndex) {
			Class<T> clazz;
			try {
				clazz = (Class<T>)Class.forName(item.className());
				final String name = clazz.getAnnotation(NameConfig.Name.class).value();
				final String type = prefix + "." + name;

				final Constructor<T> constructor = clazz.getDeclaredConstructor();

				final HashMap<String, Field> parameters = new HashMap<>();
				final HashMap<String, String> parameterNames = new HashMap<>();
				final ArrayList<Field> fields = getDeclaredFields(clazz);
				for (final Field field : fields) {
					final NameConfig.Parameter parameter = field.getAnnotation(NameConfig.Parameter.class);
					if (parameter != null) {

						final String parameterName;
						if (parameter.value().equals(""))
							parameterName = field.getName();
						else
							parameterName = parameter.value();

						parameterNames.put(field.getName(), parameterName);

						parameters.put(field.getName(), field);
					}
				}

				adapter.constructors.put(type, constructor);
				adapter.parameters.put(type, parameters);
				adapter.parameterNames.put(type, parameterNames);
			} catch (final ClassNotFoundException | NoSuchMethodException | ClassCastException
						   | UnsatisfiedLinkError e) {

				System.err.println("T '" + item.className() + "' could not be registered");
				e.printStackTrace(System.err);
			}
		}
	}

	private final Class<T> type;

	public NameConfigAdapter(Class<T> cls) {
		this.type = cls;
	}

	@Override
	public JsonElement serialize(
			final T object,
			final Type typeOfSrc,
			final JsonSerializationContext context) {

		final Class<T> clazz = (Class<T>)object.getClass();

		final String name = clazz.getAnnotation(NameConfig.Name.class).value();
		final String prefix = type.getAnnotation(NameConfig.Prefix.class).value();
		final String type = prefix + "." + name;

		final JsonObject json = new JsonObject();
		json.addProperty("name", name);
		final JsonObject configuration = new JsonObject();

		final HashMap<String, Field> parameterTypes = parameters.get(type);
		final HashMap<String, String> parameterNameMap = parameterNames.get(type);
		try {
			for (final Entry<String, Field> parameterType : parameterTypes.entrySet()) {
				final String fieldName = parameterType.getKey();
				final Field field = clazz.getDeclaredField(fieldName);
				final boolean isAccessible = field.isAccessible();
				field.setAccessible(true);
				final Object value = field.get(object);
				field.setAccessible(isAccessible);
				final JsonElement serialized = context.serialize(value);
				if (field.getAnnotation(N5Annotations.ReverseArray.class) != null) {
					final JsonArray reversedArray = reverseJsonArray(serialized.getAsJsonArray());
					configuration.add(parameterNameMap.get(fieldName), reversedArray);
				} else
					configuration.add(parameterNameMap.get(fieldName), serialized);

			}
			if (!configuration.isEmpty())
				json.add("configuration", configuration);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			new RuntimeException("Could not serialize " + clazz.getName(), e).printStackTrace(System.err);
			return null;
		}

		return json;
	}

	@Override
	public T deserialize(
			final JsonElement json,
			final Type typeOfT,
			final JsonDeserializationContext context) throws JsonParseException {

		final String prefix = type.getAnnotation(NameConfig.Prefix.class).value();

		final JsonObject objectJson = json.getAsJsonObject();
		final String name = objectJson.getAsJsonPrimitive("name").getAsString();
		if (name == null) {
			return null;
		}

		final String type = prefix + "." + name;

		final JsonObject configuration = objectJson.getAsJsonObject("configuration");
		/* It's ok to be null if all parameters are optional.
		* Otherwise, return*/
		if (configuration == null) {
			for (final Field field : parameters.get(type).values()) {
				if (!field.getAnnotation(NameConfig.Parameter.class).optional())
					return null;
			}
		}

		final Constructor<? extends T> constructor = constructors.get(type);
		constructor.setAccessible(true);
		final T object;
		try {
			object = constructor.newInstance();
			final HashMap<String, Field> parameterTypes = parameters.get(type);
			final HashMap<String, String> parameterNameMap = parameterNames.get(type);
			for (final Entry<String, Field> parameterType : parameterTypes.entrySet()) {
				final String fieldName = parameterType.getKey();
				final String paramName = parameterNameMap.get(fieldName);
				final JsonElement paramJson = configuration == null ? null : configuration.get(paramName);
				final Field field = parameterType.getValue();
				if (paramJson != null) {
					final Object parameter;
					if (field.getAnnotation(N5Annotations.ReverseArray.class) != null) {
						final JsonArray reversedArray = reverseJsonArray(paramJson);
						parameter = context.deserialize(reversedArray, field.getType());
					} else
						parameter = context.deserialize(paramJson, field.getType());
					ReflectionUtils.setFieldValue(object, fieldName, parameter);
				} else if (!field.getAnnotation(NameConfig.Parameter.class).optional()) {
					/* if param is null, and not optional, return null */
					return null;
				}
			}
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				 | SecurityException | NoSuchFieldException e) {
			e.printStackTrace(System.err);
			return null;
		}

		return object;
	}

	private static JsonArray reverseJsonArray(JsonElement paramJson) {

		final JsonArray reversedJson = new JsonArray(paramJson.getAsJsonArray().size());
		for (int i = paramJson.getAsJsonArray().size() - 1; i >= 0; i--) {
			reversedJson.add(paramJson.getAsJsonArray().get(i));
		}
		return reversedJson;
	}

	public static <T> NameConfigAdapter<T> getJsonAdapter(Class<T> cls) {

		if (adapters.get(cls) == null)
			registerAdapter(cls);
		return (NameConfigAdapter<T>) adapters.get(cls);
	}
}