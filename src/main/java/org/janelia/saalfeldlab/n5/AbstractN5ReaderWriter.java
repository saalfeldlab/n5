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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/**
 * Implementations of common methods of the {@link N5Reader} and {@link N5Writer} interfaces.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public abstract class AbstractN5ReaderWriter implements N5Reader, N5Writer {

	protected static final String jsonFile = "attributes.json";

	protected final Gson gson;

	/**
	 * Opens an {@link N5Reader}/{@link N5Writer} with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param gsonBuilder
	 */
	public AbstractN5ReaderWriter(final GsonBuilder gsonBuilder) {

		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeAdapter(CompressionType.class, new CompressionType.JsonAdapter());
		this.gson = gsonBuilder.create();
	}

	/**
	 * Opens an {@link N5Reader}/{@link N5Writer} with a default {@link GsonBuilder}.
	 *
	 * @param gsonBuilder
	 */
	public AbstractN5ReaderWriter() {

		this(new GsonBuilder());
	}

	@Override
	public void setDatasetAttributes(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		setAttributes(pathName, datasetAttributes.asMap());
	}

	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);

		final long[] dimensions = getAttribute(map, DatasetAttributes.dimensionsKey, long[].class);
		if (dimensions == null)
			return null;

		final DataType dataType = getAttribute(map, DatasetAttributes.dataTypeKey, DataType.class);
		if (dataType == null)
			return null;

		int[] blockSize = getAttribute(map, DatasetAttributes.blockSizeKey, int[].class);
		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		CompressionType compressionType = getAttribute(map, DatasetAttributes.compressionTypeKey, CompressionType.class);
		if (compressionType == null)
			compressionType = CompressionType.RAW;

		return new DatasetAttributes(dimensions, blockSize, dataType, compressionType);
	}

	@Override
	public boolean datasetExists(final String pathName) throws IOException {

		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}

	@Override
	public void createDataset(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException {

		createGroup(pathName);
		setDatasetAttributes(pathName, datasetAttributes);
	}

	@Override
	public void createDataset(
			final String pathName,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final CompressionType compressionType) throws IOException {

		createGroup(pathName);
		setDatasetAttributes(pathName, new DatasetAttributes(dimensions, blockSize, dataType, compressionType));
	}

	@Override
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		return getAttribute(map, key, clazz);
	}

	@Override
	public <T> void setAttribute(
			final String pathName,
			final String key,
			final T attribute) throws IOException {

		setAttributes(pathName, Collections.singletonMap(key, attribute));
	}

	/**
	 * Extracts an attribute from the given attributes map.
	 *
	 * @param map
	 * @param key
	 * @param clazz
	 * @return
	 * @throws IOException
	 */
	protected <T> T getAttribute(
			final HashMap<String, JsonElement> map,
			final String key,
			final Class<T> clazz) throws IOException {

		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	/**
	 * Inserts new attributes into the given attributes map.
	 *
	 * @param map
	 * @param attributes
	 * @throws IOException
	 */
	protected void insertAttributes(
			final HashMap<String, JsonElement> map,
			final Map<String, ?> attributes) throws IOException {

		for (final Entry<String, ?> entry : attributes.entrySet())
			map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));
	}

	/**
	 * Reads the attributes map from a given {@link Reader}.
	 *
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	protected HashMap<String, JsonElement> readAttributes(final Reader reader) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		final HashMap<String, JsonElement> map = gson.fromJson(reader, mapType);
		return map == null ? new HashMap<>() : map;
	}

	/**
	 * Writes the attributes map to a given {@link Writer}.
	 *
	 * @param writer
	 * @param map
	 * @throws IOException
	 */
	protected void writeAttributes(
			final Writer writer,
			final HashMap<String, JsonElement> map) throws IOException {

		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		gson.toJson(map, mapType, writer);
		writer.flush();
	}

	/**
	 * Reads a {@link DataBlock} from a given {@link ReadableByteChannel}.
	 *
	 * @param channel
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	protected DataBlock<?> readBlock(
			final ReadableByteChannel channel,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final DataInputStream dis = new DataInputStream(Channels.newInputStream(channel));
		final short mode = dis.readShort();
		final int nDim = dis.readShort();
		final int[] blockSize = new int[nDim];
		for (int d = 0; d < nDim; ++d)
			blockSize[d] = dis.readInt();
		final int numElements;
		switch (mode) {
		case 1:
			numElements = dis.readInt();
			break;
		default:
			numElements = DataBlock.getNumElements(blockSize);
		}
		final DataBlock<?> dataBlock = datasetAttributes.getDataType().createDataBlock(blockSize, gridPosition, numElements);

		final BlockReader reader = datasetAttributes.getCompressionType().getReader();
		reader.read(dataBlock, channel);
		return dataBlock;
	}

	/**
	 * Writes a {@link DataBlock} to a given {@link WritableByteChannel}.
	 *
	 * @param channel
	 * @param datasetAttributes
	 * @param dataBlock
	 * @throws IOException
	 */
	protected <T> void writeBlock(
			final WritableByteChannel channel,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final DataOutputStream dos = new DataOutputStream(Channels.newOutputStream(channel));

		final int mode = (dataBlock.getNumElements() == DataBlock.getNumElements(dataBlock.getSize())) ? 0 : 1;
		dos.writeShort(mode);

		dos.writeShort(datasetAttributes.getNumDimensions());
		for (final int size : dataBlock.getSize())
			dos.writeInt(size);

		if (mode != 0)
			dos.writeInt(dataBlock.getNumElements());

		dos.flush();

		final BlockWriter writer = datasetAttributes.getCompressionType().getWriter();
		writer.write(dataBlock, channel);
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	protected Path getDataBlockPath(
			final String datasetPathName,
			final long[] gridPosition) {

		final String[] pathComponents = new String[gridPosition.length];
		for (int i = 0; i < pathComponents.length; ++i)
			pathComponents[i] = Long.toString(gridPosition[i]);

		return Paths.get(datasetPathName, pathComponents);
	}

	/**
	 * Constructs the path for the attributes file of a group or dataset.
	 *
	 * @param pathName
	 * @return
	 */
	protected Path getAttributesPath(final String pathName) {

		return Paths.get(pathName, jsonFile);
	}
}
