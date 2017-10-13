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

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.AbstractN5ReaderWriter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Filesystem N5 implementation.
 *
 * @author Stephan Saalfeld
 */
public class N5FSReaderWriter extends AbstractN5ReaderWriter {

	protected final String basePath;

	/**
	 * Opens an {@link N5Reader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 */
	public N5FSReaderWriter(final String basePath, final GsonBuilder gsonBuilder) {

		super(gsonBuilder);
		this.basePath = basePath;
	}

	/**
	 * Opens an {@link N5Reader} at a given base path.
	 *
	 * If the base path does not exist, it will not be created and all
	 * subsequent attempts to read or write attributes, groups, or datasets
	 * will fail with an {@link IOException}.
	 *
	 * @param basePath n5 base path
	 */
	public N5FSReaderWriter(final String basePath) {

		super();
		this.basePath = basePath;
	}

	@Override
	public void createContainer() throws IOException {

		Files.createDirectories(Paths.get(basePath));
	}

	@Override
	public void removeContainer() throws IOException {

		remove("");
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
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, getAttributesPath(pathName).toString());
		if (exists(pathName) && !Files.exists(path))
			return new HashMap<>();

		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
			return readAttributes(Channels.newReader(lockedFileChannel.getFileChannel(), "UTF-8"));
		}
	}

	@Override
	public void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException {

		final Path path = Paths.get(basePath, getAttributesPath(pathName).toString());
		final HashMap<String, JsonElement> map = new HashMap<>();

		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path)) {
			map.putAll(readAttributes(Channels.newReader(lockedFileChannel.getFileChannel(), "UTF-8")));
			insertAttributes(map, attributes);

			lockedFileChannel.getFileChannel().truncate(0);
			writeAttributes(Channels.newWriter(lockedFileChannel.getFileChannel(), "UTF-8"), map);
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
}
