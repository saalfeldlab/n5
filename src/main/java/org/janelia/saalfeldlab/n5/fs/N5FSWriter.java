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
package org.janelia.saalfeldlab.n5.fs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Writer;

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

	@Override
	public <T> void setAttribute(final String pathName, final String key, final T attribute) throws IOException {

		setAttributes(pathName, Collections.singletonMap(key, attribute));
	}

	@Override
	public void setAttributes(final String pathName, final Map<String, ?> attributes) throws IOException {

		final Path path = Paths.get(basePath, pathName, jsonFile);
		try (final LockedFileChannel channel = LockedFileChannel.openForWriting(path)) {
			final Type mapType = new TypeToken<HashMap<String, JsonElement>>(){}.getType();
			HashMap<String, JsonElement> map = gson.fromJson(Channels.newReader(channel.getFileChannel(), "UTF-8"), mapType);
			if (map == null)
				map = new HashMap<>();
			for (final Entry<String, ?> entry : attributes.entrySet())
				map.put(entry.getKey(), gson.toJsonTree(entry.getValue()));
			channel.getFileChannel().position(0);
			final Writer writer = Channels.newWriter(channel.getFileChannel(), "UTF-8");
			gson.toJson(map, mapType, writer);
			writer.flush();
			channel.getFileChannel().truncate(channel.getFileChannel().position());
		}
	}

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
								try (final LockedFileChannel channel = LockedFileChannel.openForWriting(childPath)) {
									Files.delete(childPath);
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
			final DatasetAttributes datasetAttributes) throws IOException {

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
			final CompressionType compressionType) throws IOException {

		createGroup(pathName);
		setDatasetAttributes(pathName, new DatasetAttributes(dimensions, blockSize, dataType, compressionType));
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final Path path = getDataBlockPath(Paths.get(basePath, pathName).toString(), dataBlock.getGridPosition());
		Files.createDirectories(path.getParent());
		try (final LockedFileChannel channel = LockedFileChannel.openForWriting(path)) {
			// ensure that the file will have correct length by truncating it first
			channel.getFileChannel().truncate(0);
			final DataOutputStream dos = new DataOutputStream(Channels.newOutputStream(channel.getFileChannel()));

			final int mode = (dataBlock.getNumElements() == DataBlock.getNumElements(dataBlock.getSize())) ? 0 : 1;
			dos.writeShort(mode);

			dos.writeShort(datasetAttributes.getNumDimensions());
			for (final int size : dataBlock.getSize())
				dos.writeInt(size);

			if (mode != 0)
				dos.writeInt(dataBlock.getNumElements());

			dos.flush();

			final BlockWriter writer = datasetAttributes.getCompressionType().getWriter();
			writer.write(dataBlock, channel.getFileChannel());
		}
	}
}
