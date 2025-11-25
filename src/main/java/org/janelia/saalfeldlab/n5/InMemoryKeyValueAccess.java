/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * An in-memory {@link KeyValueAccess}.
 */
public class InMemoryKeyValueAccess implements KeyValueAccess {

	public static final String SCHEME = "memory";
	public static final String SEPARATOR = "/";

	private static final String ROOT = "/";

	private HashMap<String,ReadData> files;
	private HashSet<String> directories;

	/**
	 * Creates a {@link InMemoryKeyValueAccess}.
	 */
	public InMemoryKeyValueAccess() {
		files = new HashMap<>();
		directories = new HashSet<>();
		directories.add(ROOT);
	}

	@Override
	public ReadData createReadData(final String normalPath) {
		return new KeyValueAccessReadData(new MemoryLazyRead(normalPath));
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) throws N5IOException {

		if (!isFile(normalPath))
			throw new N5NoSuchKeyException("No such key: " + normalPath);

		try {
			return new MemoryLockedChannel(normalPath);
		} catch (UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for reading: " + normalPath, e);
		}
	}

	@Override
	public synchronized LockedChannel lockForWriting(final String normalPath) throws N5IOException {

		try {
			return new MemoryLockedChannel(normalPath);
		} catch (UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for writing: " + normalPath, e);
		}
	}

	@Override
	public boolean isDirectory(final String normalPath) {

		return directories.contains(normalPath) ||
				(normalPath.endsWith("/") && directories.contains(normalPath.substring(0, normalPath.length() - 1)));

	}

	@Override
	public boolean isFile(final String normalPath) {

		return files.containsKey(normalPath);

	}

	@Override
	public boolean exists(final String normalPath) {

		return isDirectory(normalPath) || isFile(normalPath);
	}

	@Override
	public long size(final String normalPath) {

		if (!isFile(normalPath))
			throw new N5Exception.N5NoSuchKeyException("No such file: " + normalPath);

		try {
			return files.get(normalPath).requireLength();
		} catch (UncheckedIOException e) {
			throw new N5Exception.N5IOException(e);
		}
	}

	@Override
	public String[] listDirectories(final String normalPath) throws N5IOException {

		if (!isDirectory(normalPath))
			throw new N5IOException("Attempted to list directory that does not exist: " + normalPath);

		return listDirectoriesHelper(normalPath).toArray(new String[0]);
	}

	private ArrayList<String> listDirectoriesHelper(final String normalPath) {

		Path dir = Paths.get(normalPath);
		final ArrayList<String> children = new ArrayList<>();
		for (String p : directories) {

			Path path = Paths.get(p);
			Path parent = path.getParent();
			if (dir.equals(parent))
				children.add(parent.relativize(path).toString());
		}
		return children;
	}

	@Override
	public String[] list(final String normalPath) throws N5IOException {

		if (!isDirectory(normalPath))
			throw new N5IOException("Attempted to list directory that does not exist: " + normalPath);

		final Path dir = Paths.get(normalPath);
		final ArrayList<String> children = new ArrayList<>();
		for (String p : files.keySet()) {

			Path path = Paths.get(p);
			Path parent = path.getParent();
			if (dir.equals(parent))
				children.add(parent.relativize(path).toString());
		}

		children.addAll(listDirectoriesHelper(normalPath));
		return children.toArray(new String[0]);
	}

	/**
	 * Returns a normalized path. It ensures correctness on both Unix and
	 * Windows, otherwise {@code pathName} is treated as UNC path on Windows,
	 * and {@code Paths.get(pathName, ...)} fails with
	 * {@code InvalidPathException}.
	 *
	 * @param path
	 *            the path
	 * @return the normalized path, without leading slash
	 */
	@Override
	public String normalize(final String path) {
		return N5URI.normalizeGroupPath(path);
	}

	/**
	 * Get the parent of a path string.
	 *
	 * @param path
	 *            the path
	 * @return the parent path or null if the path has no parent
	 */
	@Override
	public String parent(final String path) {
		final String removeTrailingSlash = path.replaceAll("/+$", "");
		return N5URI.getAsUri(removeTrailingSlash).resolve("").toString();
	}

	@Override
	public synchronized void createDirectories(final String normalPath) throws N5IOException {

		try {
			String p = normalPath;
			p = p.replaceAll("/+$", "");
			while( !p.equals(ROOT)) {
				p = p.replaceAll("/+$", "");
				directories.add(p);
				p = parent(p);
			}
		} catch (UncheckedIOException e) {
			throw new N5IOException("Failed to create directories", e);
		}
	}

	@Override
	public synchronized void delete(final String normalPath) throws N5IOException {

		// disallow deletion of root
		if (normalPath.equals("/") || normalPath.equals(""))
			return;

		deleteHelper(normalPath, directories);
		deleteHelper(normalPath, files.keySet());
	}

	private void deleteHelper(String normalPath, Iterable<String> set) {

		for (final Iterator<String> it = set.iterator(); it.hasNext();) {
			String d = it.next();
			if (d.startsWith(normalPath))
				it.remove();
		}
	}

	private class MemoryLazyRead implements LazyRead {

		private final String normalKey;

		MemoryLazyRead(String normalKey) {
	        this.normalKey = normalKey;
	    }

	    @Override
	    public long size() {
	        return InMemoryKeyValueAccess.this.size(normalKey);
	    }

	    @Override
	    public ReadData materialize(final long offset, final long length) {

			if (!isFile(normalKey))
				throw new N5NoSuchKeyException("No such key: " + normalKey);

			try {
				final ReadData rd = files.get(normalKey);
				if (length > Integer.MAX_VALUE)
					throw new IOException("Attempt to materialize too large data");

				final long channelSize = rd.requireLength();
				if (!validBounds(channelSize, offset, length))
					throw new IndexOutOfBoundsException();

				return rd.slice(offset, length);

			} catch (IOException | UncheckedIOException e) {
				throw new N5Exception.N5IOException(e);
			}
	    }

	}

	private static boolean validBounds(long channelSize, long offset, long length) {

		if (offset < 0)
			return false;
		else if (channelSize > 0 && offset >= channelSize) // offset == 0 and arrayLength == 0 is okay
			return false;
		else if (length >= 0 && offset + length > channelSize)
			return false;

		return true;
	}

	private class MemoryLockedChannel implements LockedChannel {

		private final String key;

		private ByteArrayOutputStream os;

		MemoryLockedChannel(final String normalKey) {
			this.key = normalKey;
		}

		@Override
		public void close() throws IOException {

			files.put(key, ReadData.from(os.toByteArray()));
		}

		@Override
		public Reader newReader() throws N5IOException {

			return new InputStreamReader(newInputStream());
		}

		@Override
		public InputStream newInputStream() throws N5IOException {

			return InMemoryKeyValueAccess.this.files.get(key).inputStream();
		}

		@Override
		public Writer newWriter() throws N5IOException {

			return new OutputStreamWriter(newOutputStream());
		}

		@Override
		public OutputStream newOutputStream() throws N5IOException {

			os = new ByteArrayOutputStream();
			return os;
		}

	}

}
