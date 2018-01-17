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
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
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
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5FSWriter extends N5FSReader implements N5Writer {

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		super(basePath, gsonBuilder);
		Files.createDirectories(Paths.get(basePath));
		setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath n5 base path
	 * @param gsonBuilder
	 * @throws IOException
	 *    if the base path cannot be written to or cannot be created,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5FSWriter(final String basePath) throws IOException {

		this(basePath, new GsonBuilder());
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
			map.putAll(GsonAttributesParser.readAttributes(Channels.newReader(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), getGson()));
			GsonAttributesParser.insertAttributes(map, attributes, gson);

			lockedFileChannel.getFileChannel().truncate(0);
			GsonAttributesParser.writeAttributes(Channels.newWriter(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), map, getGson());
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
			DefaultBlockWriter.writeBlock(Channels.newOutputStream(lockedChannel.getFileChannel()), datasetAttributes, dataBlock);
		}
	}

	@Override
	public boolean remove() throws IOException {

		return remove("");
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
}
