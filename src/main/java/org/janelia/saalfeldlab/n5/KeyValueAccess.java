/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;

/**
 * Key value read primitives used by {@link N5KeyValueReader}
 * implementations. This interface implements a subset of access primitives
 * provided by {@link FileSystem} to reduce the implementation burden for
 * backends
 * lacking a {@link FileSystem} implementation (such as AWS-S3).
 *
 * @author Stephan Saalfeld
 */
public interface KeyValueAccess {

	/**
	 * Split a path string into its components.
	 *
	 * @param path
	 *            the path
	 * @return the path components
	 */
	public String[] components(final String path);

	/**
	 * Compose a path from a base uri and subsequent components.
	 *
	 * @param uri the base path uri
	 * @param components the path components
	 * @return the path
	 */
	public default String compose(final URI uri, final String... components) {
		final String[] uriComponents = new String[components.length+1];
		System.arraycopy(components, 0, uriComponents, 1, components.length);
		uriComponents[0] = uri.getPath();
		return compose(uriComponents);
	}

	public String compose(final String... components);

	/**
	 * Get the parent of a path string.
	 *
	 * @param path
	 *            the path
	 * @return the parent path or null if the path has no parent
	 */
	public String parent(final String path);

	/**
	 * Relativize path relative to base.
	 *
	 * @param path
	 *            the path
	 * @param base
	 *            the base path
	 * @return the result or null if the path has no parent
	 */
	public String relativize(final String path, final String base);

	/**
	 * Normalize a path to canonical form. All paths pointing to the same
	 * location return the same output. This is most important for cached
	 * data pointing at the same location getting the same key.
	 *
	 * @param path
	 *            the path
	 * @return the normalized path
	 */
	public String normalize(final String path);

	/**
	 * Get the absolute (including scheme) {@link URI} of the given path
	 *
	 * @param uriString
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return absolute URI
	 * @throws URISyntaxException if the given path is not a proper URI
	 */
	public URI uri(final String uriString) throws URISyntaxException;
	/**
	 * Test whether the path exists.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path exists
	 */
	public boolean exists(final String normalPath);

	public long size(final String normalPath) throws IOException;

	/**
	 * Test whether the path is a directory.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path is a directory
	 */
	public boolean isDirectory(String normalPath);

	/**
	 * Test whether the path is a file.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path is a file
	 */
	public boolean isFile(String normalPath); // TODO: Looks un-used. Remove?

	/**
	 * Create a lock on a path for reading. This isn't meant to be kept
	 * around. Create, use, [auto]close, e.g.
	 * <code>
	 * try (final lock = store.lockForReading()) {
	 *   ...
	 * }
	 * </code>
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the locked channel
	 * @throws IOException
	 *             if a locked channel could not be created
	 */
	public LockedChannel lockForReading(final String normalPath) throws IOException;

	public LockedChannel lockForReading(String normalPath, final long startByte, final long size)
			throws IOException;

	/**
	 * Create an exclusive lock on a path for writing. If the file doesn't
	 * exist yet, it will be created, including all directories leading up to
	 * it. This lock isn't meant to be kept around. Create, use, [auto]close,
	 * e.g.
	 * <code>
	 * try (final lock = store.lockForWriting()) {
	 *   ...
	 * }
	 * </code>
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the locked channel
	 * @throws IOException
	 *             if a locked channel could not be created
	 */
	public LockedChannel lockForWriting(final String normalPath) throws IOException;

	public LockedChannel lockForWriting(String normalPath, final long startByte, final long size)
			throws IOException;

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the directories
	 * @throws IOException
	 *             if an error occurs during listing
	 */
	public String[] listDirectories(final String normalPath) throws IOException;

	/**
	 * List all children of a path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the the child paths
	 * @throws IOException if an error occurs during listing
	 */
	public String[] list(final String normalPath) throws IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @throws IOException
	 *             if an error occurs during creation
	 */
	public void createDirectories(final String normalPath) throws IOException;

	/**
	 * Delete a path. If the path is a directory, delete it recursively.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @throws IOException
	 *            if an error occurs during deletion
	 */
	public void delete(final String normalPath) throws IOException;

}
