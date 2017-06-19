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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/**
 * A simple structured container format for hierarchies of chunked
 * n-dimensional datasets and attributes.
 *
 * {@linkplain https://github.com/axtimwalde/n5}
 *
 * @author Stephan Saalfeld
 */
public class N5FSWriter extends N5FSReader implements N5Writer {

	/**
	 * Opens an {@link N5Writer} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read or write attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * If the base path is not writable, all subsequent attempts to write
	 * attributes, groups, or datasets will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder) {
		super(basePath, gsonBuilder);
	}

	/**
	 * Opens an {@link N5Writer} at a given base path.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read or write attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * If the base path is not writable, all subsequent attempts to write
	 * attributes, groups, or datasets will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 */
	public N5FSWriter(final String basePath) {
		this(basePath, new GsonBuilder());
	}

	private static class LockedFileChannel {

		private final FileChannel channel;
		private final FileLock lock;

		LockedFileChannel(final Path path) throws IOException {

			@SuppressWarnings("hiding")
			FileChannel channel = null;
			@SuppressWarnings("hiding")
			FileLock lock = null;
			for (boolean waiting = true; waiting;) {
				waiting = false;
				try {
					channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
					lock = channel.lock();
				} catch (final OverlappingFileLockException e) {
					waiting = true;
					try {
						Thread.sleep(100);
					} catch (final InterruptedException f) {
						channel = null;
						lock = null;
						waiting = false;
						f.printStackTrace(System.err);
					}
				} catch (final IOException e) {
					channel = FileChannel.open(path, StandardOpenOption.READ);
					lock = null;
				}
			}

			this.channel = channel;
			this.lock = lock;
		}

		private void close() throws IOException {

			if (lock != null)
				lock.release();
			channel.close();
		}
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 *
	 * TODO uses file locks to synchronize with other processes, now also
	 *   synchronize for threads inside the JVM
	 */
	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {
		final Path path = Paths.get(basePath, pathName, jsonFile);
		final LockedFileChannel lockedFileChannel = new LockedFileChannel(path);
		final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
		HashMap<String, JsonElement> map = gson.fromJson(Channels.newReader(lockedFileChannel.channel, "UTF-8"), mapType);
		if (map == null)
			map = new HashMap<>();
		lockedFileChannel.close();
		return map;
	}

	/**
	 * Reads an attribute.
	 *
	 * @param pathName group path
	 * @param key
	 * @param clazz attribute class
	 * @return
	 */
	@Override
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
	 * @param pathName group path
	 * @param key
	 * @param attribute
	 * @throws IOException
	 */
	@Override
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
	 * @param pathName group path
	 * @param attributes
	 * @throws IOException
	 */
	@Override
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
	 * @param pathName dataset path
	 * @param datasetInfo
	 * @throws IOException
	 */
	@Override
	public void setDatasetAttributes(final String pathName, final DatasetAttributes datasetInfo) throws IOException {
		setAttributes(pathName, datasetInfo.asMap());
	}

	/**
	 * Get mandatory dataset attributes.
	 *
	 * @param pathName dataset path
	 * @return dataset attributes or null if either dimensions or dataType are not set
	 * @throws IOException
	 */
	@Override
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException {

		final HashMap<String, JsonElement> attributes = getAttributes(pathName);

		final JsonElement dimensionsElement = attributes.get(DatasetAttributes.dimensionsKey);
		if (dimensionsElement == null)
			return null;
		final long[] dimensions = gson.fromJson(dimensionsElement, long[].class);
		if (dimensions == null)
			return null;

		final JsonElement dataTypeElement = attributes.get(DatasetAttributes.dataTypeKey);
		if (dataTypeElement == null)
			return null;
		final DataType dataType = gson.fromJson(dataTypeElement, DataType.class);
		if (dataType == null)
			return null;

		final JsonElement blockSizeElement = attributes.get(DatasetAttributes.blockSizeKey);
		int[] blockSize = null;
		if (blockSizeElement != null)
			blockSize = gson.fromJson(blockSizeElement, int[].class);
		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		final JsonElement compressionTypeElement = attributes.get(DatasetAttributes.compressionTypeKey);
		CompressionType compressionType = null;
		if (compressionTypeElement == null)
			return null;
		compressionType = gson.fromJson(compressionTypeElement, CompressionType.class);
		if (compressionType == null)
			compressionType = CompressionType.RAW;

		return new DatasetAttributes(dimensions, blockSize, dataType, compressionType);
	}



	/**
	 * Creates a group (directory)
	 *
	 * @param pathName
	 * @throws IOException
	 */
	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, pathName);
		Files.createDirectories(path);
	}

	/**
	 * Removes a group or dataset (directory and all contained files).
	 *
	 * <p><code>{@link #remove(String) remove("")}</code> or
	 * <code>{@link #remove(String) remove("")}</code> will delete this N5
	 * container.  Please note that no checks for safety will be performed,
	 * e.g. <code>{@link #remove(String) remove("..")}</code> will try to
	 * recursively delete the parent directory of this N5 container which
	 * only fails because it attempts to delete the parent directory before it
	 * is empty.
	 *
	 * @param pathName group path
	 * @throws IOException
	 */
	@Override
	public boolean remove(final String pathName) throws IOException {
		final Path path = Paths.get(basePath, pathName);
		if (Files.exists(path))
			try (final Stream<Path> pathStream = Files.walk(path)) {
				pathStream.sorted(Comparator.reverseOrder()).forEach(
						childPath -> {
							if (Files.isRegularFile(childPath)) {
								try (final FileChannel channel = FileChannel.open(childPath, StandardOpenOption.WRITE)) {
									final FileLock lock = channel.lock();
    									Files.delete(childPath);
									if (lock.isValid()) lock.release();
								} catch (final IOException e) {
									e.printStackTrace();
								}
							} else
								try {
									Files.delete(childPath);
								} catch (final IOException e) {
									e.printStackTrace();
								}
						});
			}

		return !Files.exists(path);
	}

	/**
	 * Creates a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes
	 * @throws IOException
	 */
	@Override
	public void createDataset(
			final String pathName,
			final DatasetAttributes datasetAttributes) throws IOException{
		createGroup(pathName);
		setDatasetAttributes(pathName, datasetAttributes);
	}

	/**
	 * Creates a dataset.  This does not create any data but the path and
	 * mandatory attributes only.
	 *
	 * @param pathName dataset path
	 * @param dimensions
	 * @param blockSize
	 * @param dataType
	 * @throws IOException
	 */
	@Override
	public void createDataset(
			final String pathName,
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final CompressionType compressionType) throws IOException{
		createGroup(pathName);
		setDatasetAttributes(pathName, new DatasetAttributes(dimensions, blockSize, dataType, compressionType));
	}

	/**
	 * Writes a {@link DataBlock}.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes
	 * @param dataBlock
	 * @throws IOException
	 */
	@Override
	public < T > void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock< T > dataBlock ) throws IOException {

		final Path path = DataBlock.getPath(Paths.get(basePath, pathName).toString(), dataBlock.getGridPosition());
		Files.createDirectories(path.getParent());
		final File file = path.toFile();
		try (final FileOutputStream out = new FileOutputStream(file)) {
			final FileChannel channel = out.getChannel();
			final FileLock lock = channel.lock();
			final DataOutputStream dos = new DataOutputStream(out);
			dos.writeInt(datasetAttributes.getNumDimensions());
			for (final int size : dataBlock.getSize())
				dos.writeInt(size);

			dos.flush();

			final BlockWriter writer = datasetAttributes.getCompressionType().getWriter();
			writer.write(dataBlock, channel);
			if (lock.isValid()) lock.release();
		}
	}

	/**
	 * Reads a {@link DataBlock}.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	@Override
	public DataBlock< ? > readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition ) throws IOException {

		final Path path = DataBlock.getPath(Paths.get(basePath, pathName).toString(), gridPosition);
		final File file = path.toFile();
		if (!file.exists())
			return null;
		final LockedFileChannel lockedChannel = new LockedFileChannel(path);

		try (final InputStream in = Channels.newInputStream(lockedChannel.channel)) {
			final DataInputStream dis = new DataInputStream(in);
			final int nDim = dis.readInt();
			final int[] blockSize = new int[nDim];
			for (int d = 0; d < nDim; ++d)
				blockSize[d] = dis.readInt();
			final DataBlock<?> dataBlock = datasetAttributes.getDataType().createDataBlock(blockSize, gridPosition);

			final BlockReader reader = datasetAttributes.getCompressionType().getReader();
			reader.read(dataBlock, lockedChannel.channel);
			if (lockedChannel.lock != null && lockedChannel.lock.isValid()) lockedChannel.lock.release();
			return dataBlock;
		}
	}

	/**
	 * Test whether a group or dataset exists.
	 *
	 * @param pathName group path
	 * @return
	 */
	@Override
	public boolean exists(final String pathName) {
		final Path path = Paths.get(basePath, pathName);
		return Files.exists(path) && Files.isDirectory(path);
	}

	/**
	 * Test whether a dataset exists.
	 *
	 * @param pathName dataset path
	 * @return
	 */
	@Override
	public boolean datasetExists(final String pathName) throws IOException {
		return exists(pathName) && getDatasetAttributes(pathName) != null;
	}
}
