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

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Enumerates available compression types, and the corresponding block readers
 * and writers.
 *
 * @author Stephan Saalfeld
 */
public enum CompressionScheme {

	RAW("raw", new RawCompression(), new RawCompression()),
	BZIP2("bzip2", new Bzip2BlockReaderWriter(), new Bzip2BlockReaderWriter()),
	GZIP("gzip", new GzipCompression(), new GzipCompression()),
	LZ4("lz4", new Lz4BlockReaderWriter(), new Lz4BlockReaderWriter()),
	XZ("xz", new XzBlockReaderWriter(), new XzBlockReaderWriter());

	private final String label;
	private final BlockReader reader;
	private final BlockWriter writer;

	private CompressionScheme(final String label, final BlockReader reader, final BlockWriter writer) {

		this.label = label;
		this.reader = reader;
		this.writer = writer;
	}

	@Override
	public String toString() {

		return label;
	}

	public static CompressionScheme fromString(final String string) {

		for (final CompressionScheme value : values())
			if (value.toString().equals(string))
				return value;
		return null;
	}

	public BlockReader getReader() {

		return reader;
	}

	public BlockWriter getWriter() {

		return writer;
	}

	public static class JsonAdapter implements JsonDeserializer<CompressionScheme>, JsonSerializer<CompressionScheme> {

		@Override
		public CompressionScheme deserialize(
				final JsonElement json,
				final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {

			return CompressionScheme.fromString(json.getAsString());
		}

		@Override
		public JsonElement serialize(
				final CompressionScheme src,
				final Type typeOfSrc,
				final JsonSerializationContext context) {

			return new JsonPrimitive(src.toString());
		}
	}
}