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
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Abstract base class implementing {@link N5Reader} with JSON attributes
 * parsed with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public abstract class AbstractGsonReader implements GsonAttributesParser, N5Reader {

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

		return getAttribute(pathName, key, TypeToken.get(clazz).getType());
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Type type) throws IOException {
		final String normalizedGroupPath;
		final String normalizedAttributePath;
		try {
			final N5URL n5url = N5URL.from(null, pathName, key );
			normalizedGroupPath = n5url.normalizeGroupPath();
			normalizedAttributePath = n5url.normalizeAttributePath();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return GsonAttributesParser.readAttribute( getAttributesJson( normalizedGroupPath ), normalizedAttributePath, type,  gson);
	}

}
