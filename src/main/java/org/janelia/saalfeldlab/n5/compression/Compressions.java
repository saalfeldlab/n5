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
package org.janelia.saalfeldlab.n5.compression;

import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.janelia.saalfeldlab.n5.compression.Compression.CompressionType;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;

/**
 * Compression registry.
 *
 * @author Stephan Saalfeld
 */
public class CompressionAdapter implements JsonDeserializer<Compression>, JsonSerializer<Compression> {

	private static CompressionAdapter instance;
	private final HashMap<Class<? extends Compression>, JsonSerializer<? extends Compression>> compressionSerializers = new HashMap<>();
	private final HashMap<String, JsonDeserializer<?>> compressionDeserializers = new HashMap<>();

	private CompressionAdapter() {

		update();
	}

	public static CompressionAdapter getInstance() {

		if (instance == null)
			instance = new CompressionAdapter();

		return instance;
	}

	private static void getDeclaredFields(Class<?> clazz) {

		Field[] fields = clazz.getDeclaredFields();
		clazz = clazz.getSuperclass();
		while (clazz != null) {
			fields = ArrayUtils.add(fields, clazz.getDeclaredFields());
			clazz = clazz.getSuperclass();

		Class<?> current = yourClass;
		while(current.getSuperclass()!=null){ // we don't want to process Object.class
		    // do something with current's fields
		    current = current.getSuperclass();
		}
	}

	public void update() {

		compressionSerializers.clear();
		compressionDeserializers.clear();

		final FastClasspathScanner scanner = new FastClasspathScanner();
		scanner.setAnnotationVisibility(RetentionPolicy.RUNTIME);
		final ScanResult result = scanner.scan(Runtime.getRuntime().availableProcessors());
		List<String> compressionClasses = result..getNamesOfClassesWithAnnotation(CompressionType.class);
		for (final String className : compressionClasses) {
			Class<? extends Compression> clazz;
			try {
				clazz = (Class<? extends Compression>)Class.forName(className);
				Constructor<? extends Compression>[] constructors = (Constructor<? extends Compression>[])clazz.getDeclaredConstructors();
				clazz.
				defaultConstructor
				Arrays.sort(constructors, );
				compressionSerializers.put(
						clazz,
						(Compression src, Type typeOfSrc, JsonSerializationContext context) -> {
							clazz.
							clazz.getDeclaredMethod(name, parameterTypes)
							// TODO write specific serializer for each Compression
							return null;
						});
				compressionDeserializers.put(
						clazz.getAnnotation(CompressionType.class).value(),
						(JsonElement json, Type typeOfT, JsonDeserializationContext context) -> {

							// TODO write specific serializer for each Compression
							return null;
						});
			} catch (final ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public JsonElement serialize(Compression src, Type typeOfSrc, JsonSerializationContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Compression deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		// TODO Auto-generated method stub
		return null;
	}
}