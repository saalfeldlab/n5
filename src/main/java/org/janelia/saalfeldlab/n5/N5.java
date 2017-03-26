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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 *
 *
 * @author Stephan Saalfeld
 */
public class N5
{
	private static final String jsonFile = "attributes.json";
	static final String dimensionsKey = "dimensions";
	static final String blockSizeKey = "blockSize";
	static final String dataTypeKey = "dataType";
	static final String compressionTypeKey = "compressionType";

	private final KeySetView<Object, Boolean> threadLocks = ConcurrentHashMap.newKeySet();

	public enum DataType {

		UINT8("uint8"),
		UINT16("uint16"),
		UINT32("uint32"),
		UINT64("uint64"),
		INT8("int8"),
		INT16("int16"),
		INT32("int32"),
		INT64("int64"),
		FLOAT32("float32"),
		FLOAT64("float64");

		private final String label;

		private DataType(final String label) {
			this.label = label;
		}

		@Override
		public String toString() {
			return label;
		}

		public static DataType fromString(final String string) {
			for (final DataType value : values())
				if (value.toString().equals(string))
					return value;
			return null;
		}
	}

	private class DataTypeJsonAdapter implements JsonDeserializer<DataType>, JsonSerializer<DataType>
	{
		@Override
		public DataType deserialize(
				final JsonElement json,
				final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {
			return DataType.fromString(json.getAsString());
		}

		@Override
		public JsonElement serialize(
				final DataType src,
				final Type typeOfSrc,
				final JsonSerializationContext context) {
			return new JsonPrimitive(src.toString());
		}
	}

	private class CompressionTypeJsonAdapter implements JsonDeserializer<CompressionType>, JsonSerializer<CompressionType>
	{
		@Override
		public CompressionType deserialize(
				final JsonElement json,
				final Type typeOfT,
				final JsonDeserializationContext context) throws JsonParseException {
			return CompressionType.fromString(json.getAsString());
		}

		@Override
		public JsonElement serialize(
				final CompressionType src,
				final Type typeOfSrc,
				final JsonSerializationContext context) {
			return new JsonPrimitive(src.toString());
		}
	}

	public enum CompressionType {

		RAW("raw", new RawBlockWriter()),
		GZIP("gzip", new GzipBlockWriter()),
		BZIP2("bzip2", new Bzip2BlockWriter());

		private final String label;
		private final BlockWriter writer;

		private CompressionType(final String label, final BlockWriter writer) {
			this.label = label;
			this.writer = writer;
		}

		@Override
		public String toString() {
			return label;
		}

		public static CompressionType fromString(final String string) {
			for (final CompressionType value : values())
				if (value.toString().equals(string))
					return value;
			return null;
		}

		public BlockWriter getWriter() {
			return writer;
		}
	}

	final private Gson gson;
	final private String basePath;

	public N5(final String basePath, final GsonBuilder gsonBuilder) {
		this.basePath = basePath;
		gsonBuilder.registerTypeAdapter(DataType.class, new DataTypeJsonAdapter());
		gsonBuilder.registerTypeAdapter(CompressionType.class, new CompressionTypeJsonAdapter());
		this.gson = gsonBuilder.create();
	}

	public N5(final String basePath) {
		this(basePath, new GsonBuilder());
	}

	/**
	 * Returns or creates attributes map of a group or dataset.
	 *
	 * @param path
	 * @return
	 * @throws IOException
	 *
	 * TODO uses file locks to synchronize with other processes, now also
	 *   synchronize for threads inside the JVM
	 */
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {
		final Path path = Paths.get(basePath, pathName, jsonFile);
		FileLock fileLock;
		FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
			fileLock = channel.lock();
		} catch (final IOException e) {
			channel = FileChannel.open(path, StandardOpenOption.READ);
			fileLock = null;
		}
		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		HashMap<String, JsonElement> map = gson.fromJson(Channels.newReader(channel, "UTF-8"), mapType);
		if (map == null)
			map = new HashMap<>();
		if (fileLock != null)
			fileLock.release();
		channel.close();
		return map;
	}

	/**
	 * Return an attribute.
	 *
	 * @param path
	 * @return
	 */
	public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz) throws IOException {
		final HashMap<String, JsonElement> map = getAttributes(pathName);
		final JsonElement attribute = map.get(key);
		if (attribute != null)
			return gson.fromJson(attribute, clazz);
		else
			return null;
	}

	/**
	 * Sets an attribute.
	 *
	 * @param pathName
	 * @param key
	 * @param attribute
	 * @throws IOException
	 */
	public <T> void setAttribute(final String pathName, final String key, final T attribute) throws IOException {
		final Path path = Paths.get(basePath, pathName, jsonFile);
		try (final FileChannel channel = FileChannel.open(
				path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
			final FileLock lock = channel.lock();
			final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
			HashMap<String, JsonElement> map = gson.fromJson(Channels.newReader(channel, "UTF-8"), mapType);
			if (map == null)
				map = new HashMap<>();
			map.put(key, gson.toJsonTree(attribute, new TypeToken<T>(){}.getType()));
			channel.position(0);
			final Writer writer = Channels.newWriter(channel, "UTF-8");
			gson.toJson(map, mapType, writer);
			writer.flush();
			channel.truncate(channel.position());
			lock.release();
		}
	}

	/**
	 * Sets a map of attributes.
	 *
	 * @param pathName
	 * @param attributes
	 * @throws IOException
	 */
	public void setAttributes(final String pathName, final Map<String, ?> attributes) throws IOException {
		final Path path = Paths.get(basePath, pathName, jsonFile);
		try (final FileChannel channel = FileChannel.open(
				path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
			final FileLock lock = channel.lock();
			final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
			HashMap<String, JsonElement> map = gson.fromJson(Channels.newReader(channel, "UTF-8"), mapType);
			if (map == null)
				map = new HashMap<>();
			for (final Entry<String, ?> entry : attributes.entrySet())
				map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));
			channel.position(0);
			final Writer writer = Channels.newWriter(channel, "UTF-8");
			gson.toJson(map, mapType, writer);
			writer.flush();
			channel.truncate(channel.position());
			lock.release();
		}
	}

	/**
	 * Sets mandatory dataset attributes.
	 *
	 * @param pathName
	 * @param datasetInfo
	 * @throws IOException
	 */
	public void setDatasetAttributes(final String pathName, final DatasetAttributes datasetInfo) throws IOException {
		setAttributes(pathName, datasetInfo.asMap());
	}

	/**
	 * Get mandatory dataset attributes.
	 *
	 * @param pathName
	 * @return dataset info or null if either dimensions or dataType are not set
	 * @throws IOException
	 */
	public DatasetAttributes getDatasetInfo(final String pathName) throws IOException {

		final HashMap<String, JsonElement> attributes = getAttributes(pathName);

		final JsonElement dimensionsElement = attributes.get(dimensionsKey);
		if (dimensionsElement == null)
			return null;
		final long[] dimensions = gson.fromJson(dimensionsElement, long[].class);
		if (dimensions == null)
			return null;

		final JsonElement dataTypeElement = attributes.get(dataTypeKey);
		if (dataTypeElement == null)
			return null;
		final DataType dataType = gson.fromJson(dataTypeElement, DataType.class);
		if (dataType == null)
			return null;

		final JsonElement blockSizeElement = attributes.get(blockSizeKey);
		int[] blockSize = null;
		if (blockSizeElement != null)
			blockSize = gson.fromJson(blockSizeElement, int[].class);
		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		final JsonElement compressionTypeElement = attributes.get(compressionTypeKey);
		CompressionType compressionType = null;
		if (compressionTypeElement == null)
			return null;
		compressionType = gson.fromJson(compressionTypeElement, CompressionType.class);
		if (compressionType == null)
			compressionType = CompressionType.RAW;

		return new DatasetAttributes(dimensions, blockSize, dataType, compressionType);
	}



	/**
	 * Create  group (directory)
	 *
	 * @param pathName
	 * @throws IOException
	 */
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, pathName);
		Files.createDirectories( path );
	}

	/**
	 * Remove a group or dataset (directory and all contained files)
	 *
	 * @param pathName
	 * @throws IOException
	 */
	public void remove(final String pathName) throws IOException {
		final Path path = Paths.get(basePath, pathName);
		if (path.toFile().exists())
			Files.walk(path)
		    	.sorted(Comparator.reverseOrder())
		    	.forEach(
		    			filePath -> {
		    				final File file = filePath.toFile();
		    				if (file.isFile()) {
		    					try (final FileLock lock = FileChannel.open(filePath, StandardOpenOption.WRITE).lock()) {
									file.delete();
									lock.release();
								} catch (final IOException e) {
									e.printStackTrace();
								}
		    				}
		    				else
		    					file.delete();
		    				});
	}

	/**
	 * Create a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName
	 * @param dimensions
	 * @param blockSizes
	 * @param dataType
	 * @throws IOException
	 */
	public void createDataset(
			final String pathName,
			final DatasetAttributes datasetInfo) throws IOException{
		createGroup(pathName);
		setDatasetAttributes(pathName, datasetInfo);
	}

	/**
	 * Create a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName
	 * @param dimensions
	 * @param blockSize
	 * @param dataType
	 * @throws IOException
	 */
	public void createDataset(
			final String pathName,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final CompressionType compressionType) throws IOException{
		createGroup(pathName);
		setDatasetAttributes(pathName, new DatasetAttributes(dimensions, blockSize, dataType, compressionType));
	}

	public < T > void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final AbstractDataBlock< T > dataBlock ) throws IOException {

		final Path path = dataBlock.getPath(pathName);
		Files.createDirectories(path.getParent());
		final File file = path.toFile();
		try (final FileOutputStream out = new FileOutputStream(file)) {
			final FileChannel channel = out.getChannel();
			final FileLock lock = channel.lock();
			final DataOutputStream dos = new DataOutputStream(out);
			dos.writeInt(datasetAttributes.getNumDimensions());
			for (final int size : dataBlock.size)
				dos.writeLong(size);

			final BlockWriter writer = datasetAttributes.getCompressionType().getWriter();
			writer.write(dataBlock, channel);
			channel.truncate(channel.position());
			dos.flush();
			lock.release();
		}
	}
}
