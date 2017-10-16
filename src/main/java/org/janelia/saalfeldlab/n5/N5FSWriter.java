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
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Filesystem N5 implementation.
 *
 * @author Stephan Saalfeld
 */
public class N5FSWriter extends N5FSReader implements DefaultGsonReader, N5Writer {

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path is not writable, all subsequent attempts to
	 * write attributes, groups, or datasets will fail with an
	 * {@link IOException}.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		super(basePath, gsonBuilder);
		Files.createDirectories(Paths.get(basePath));
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 *
	 * If the base path is not writable, all subsequent attempts to
	 * write attributes, groups, or datasets will fail with an
	 * {@link IOException}.
	 *
	 * @param basePath n5 base path
	 * @throws IOException
	 */
	public N5FSWriter(final String basePath) throws IOException {

		this(basePath, new GsonBuilder());
	}

	@Override
	public boolean remove() throws IOException {

		return remove("");
	}

	@Override
	public boolean exists(final String pathName) {

		final Path path = Paths.get(basePath, pathName);
		return Files.exists(path) && Files.isDirectory(path);
	}

	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, pathName);
		Files.createDirectories(path);
	}

	@Override
	public void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException {

		final Path path = Paths.get(basePath, getAttributesPath(pathName).toString());
		final HashMap<String, JsonElement> map = new HashMap<>();

		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
			map.putAll(GsonAttributesParser.readAttributes(Channels.newReader(lockedFileChannel.getFileChannel(), "UTF-8"), gson));
			GsonAttributesParser.insertAttributes(map, attributes, gson);

			lockedFileChannel.getFileChannel().truncate(0);
			GsonAttributesParser.writeAttributes(Channels.newWriter(lockedFileChannel.getFileChannel(), "UTF-8"), map, gson);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final Path path = Paths.get(basePath, getDataBlockPath(pathName, gridPosition).toString());
		if (!Files.exists(path))
			return null;

		try (final LockedFileChannel lockedChannel = LockedFileChannel.openForReading(path)) {
			return readBlock(lockedChannel.getFileChannel(), datasetAttributes, gridPosition);
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		final Path path = Paths.get(basePath, getDataBlockPath(pathName, dataBlock.getGridPosition()).toString());
		Files.createDirectories(path.getParent());
		try (final LockedFileChannel lockedChannel = LockedFileChannel.openForWriting(path)) {
			lockedChannel.getFileChannel().truncate(0);
			writeBlock(lockedChannel.getFileChannel(), datasetAttributes, dataBlock);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, pathName);
		try (final Stream<Path> pathStream = Files.list(path))
		{
			return pathStream
					.filter(a -> Files.isDirectory(a))
					.map(a -> path.relativize(a).toString())
					.toArray(n -> new String[n]);
		}
	}

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
	@Override
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
	@Override
	protected Path getAttributesPath(final String pathName) {

		return Paths.get(pathName, jsonFile);
	}
}
