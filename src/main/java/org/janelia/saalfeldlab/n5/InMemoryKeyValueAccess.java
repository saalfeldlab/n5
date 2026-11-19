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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * An in-memory {@link KeyValueAccess}.
 * <p>
 * This implementation does not implement an {@link IoPolicy}.
 */
public class InMemoryKeyValueAccess implements KeyValueAccess {

	public static final String SCHEME = "memory";
	public static final String SEPARATOR = "/";

	private static final String ROOT = "/";

	private Map<String,ReadData> files;
	private Set<String> directories;

	/**
	 * Creates a {@link InMemoryKeyValueAccess}.
	 */
	public InMemoryKeyValueAccess() {
		files = new ConcurrentHashMap<>();
		directories = Collections.synchronizedSet(new HashSet<>());
		directories.add(ROOT);
	}

	/**
	 * Returns a normalized path.
	 *
	 * @param path
	 *            the path
	 * @return the normalized path
	 */
	@Override
	public String normalize(final String path) {
		return N5URI.normalizeGroupPath(path);
	}

	@Override
	public VolatileReadData createReadData(final String normalPath) {

		return VolatileReadData.from(new MemoryLazyRead(normalPath));
	}

	@Override
	public void write(String normalPath, ReadData data) {

		files.put(normalPath, data);
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

		final ArrayList<String> children = new ArrayList<>();
		for (String d : directories) {
			// the parent method removes leading slash, so add it back
			String parent = "/" + parent(d);
			if (normalPath.equals(parent))
				children.add(relativize(d, parent));
		}
		return children;
	}

	@Override
	public String[] list(final String normalPath) throws N5IOException {

		if (!isDirectory(normalPath))
			throw new N5IOException("Attempted to list directory that does not exist: " + normalPath);

		final ArrayList<String> children = new ArrayList<>();
		// all keys are normalized
		for (String p : files.keySet()) {
			final String parent = parent(p);
			if (normalPath.equals(parent))
				children.add(relativize(p, parent));
		}

		children.addAll(listDirectoriesHelper(normalPath));
		return children.toArray(new String[0]);
	}

	@Override
	public void createDirectories(final String normalPath) throws N5IOException {

		String p = normalPath;
		while( !isRoot(p)) {
			directories.add(p);
			p = "/" + parent(p);
		}
	}

	private boolean isRoot(String path) {
		return path.isEmpty() || path.equals(ROOT);
	}

	@Override
	public void delete(final String normalPath) throws N5IOException {

		// disallow deletion of root
		if (isRoot(normalPath))
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

		@Override
		public void close() {
			// No-op
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

}
