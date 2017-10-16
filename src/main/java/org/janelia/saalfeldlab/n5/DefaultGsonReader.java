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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Default implementation of {@link N5Reader} with JSON attributes parsed
 * with {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public interface DefaultGsonReader extends GsonAttributesParser, N5Reader {

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException;

	@Override
	public default DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

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

		CompressionType compressionType = GsonAttributesParser.parseAttribute(map, DatasetAttributes.compressionTypeKey, CompressionType.class, gson);
		if (compressionType == null)
			compressionType = CompressionType.RAW;

		return new DatasetAttributes(dimensions, blockSize, dataType, compressionType);
	}

	@Override
	public default <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		return GsonAttributesParser.parseAttribute(map, key, clazz, getGson());
	}
}
