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
 * 
 * @param <T>
 *            the class this adapter (de)serializes
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

				final NameConfig.Serialize serialize = clazz.getAnnotation(NameConfig.Serialize.class);
				if (serialize != null && !serialize.value())
					continue;

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
