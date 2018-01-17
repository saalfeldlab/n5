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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.stream.Stream;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Filesystem {@link N5Reader} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5FSReader extends AbstractGsonReader {

	protected static class LockedFileChannel implements Closeable {

		private final FileChannel channel;

		public static LockedFileChannel openForReading(final Path path) throws IOException {

			return new LockedFileChannel(path, true);
		}

		public static LockedFileChannel openForWriting(final Path path) throws IOException {

			return new LockedFileChannel(path, false);
		}

		private LockedFileChannel(final Path path, final boolean readOnly) throws IOException {

			final OpenOption[] options = readOnly ? new OpenOption[]{StandardOpenOption.READ} : new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};
			channel = FileChannel.open(path, options);

			for (boolean waiting = true; waiting;) {
				waiting = false;
				try {
					channel.lock(0L, Long.MAX_VALUE, readOnly);
				} catch (final OverlappingFileLockException e) {
					waiting = true;
					try {
						Thread.sleep(100);
					} catch (final InterruptedException f) {
						waiting = false;
						f.printStackTrace(System.err);
					}
				}
			}
		}

		public FileChannel getFileChannel() {

			return channel;
		}

		@Override
		public void close() throws IOException {

			channel.close();
		}
	}

	protected static final String jsonFile = "attributes.json";

	protected final String basePath;

	/**
	 * Opens an {@link N5FSReader} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * @param basePath N5 base path
	 * @param gsonBuilder
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5FSReader(final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		super(gsonBuilder);
		this.basePath = basePath;
		if (exists("/")) {
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	/**
	 * Opens an {@link N5FSReader} at a given base path.
	 *
	 * @param basePath N5 base path
	 * @throws IOException
	 *    if the base path cannot be read or does not exist,
	 *    if the N5 version of the container is not compatible with this
	 *    implementation.
	 */
	public N5FSReader(final String basePath) throws IOException {

		this(basePath, new GsonBuilder());
	}

	@Override
	public boolean exists(final String pathName) {

		final Path path = Paths.get(basePath, pathName);
		return Files.exists(path) && Files.isDirectory(path);
	}

	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, getAttributesPath(pathName).toString());
		if (exists(pathName) && !Files.exists(path))
			return new HashMap<>();

		try (final LockedFileChannel lockedFileChannel = LockedFileChannel.openForReading(path)) {
			return GsonAttributesParser.readAttributes(Channels.newReader(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()), getGson());
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
			return DefaultBlockReader.readBlock(Channels.newInputStream(lockedChannel.getFileChannel()), datasetAttributes, gridPosition);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final Path path = Paths.get(basePath, pathName);
		try (final Stream<Path> pathStream = Files.list(path)) {
			return pathStream
					.filter(a -> Files.isDirectory(a))
					.map(a -> path.relativize(a).toString())
					.toArray(n -> new String[n]);
		}
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
	protected static Path getDataBlockPath(
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
	protected static Path getAttributesPath(final String pathName) {

		return Paths.get(pathName, jsonFile);
	}
}
