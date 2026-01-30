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

import java.io.Closeable;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

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
	default String[] components( final String path ) {

		String[] components = Arrays.stream(path.split("/"))
				.filter(x -> !x.isEmpty())
				.toArray(String[]::new);
		if (components.length == 0)
			return path.startsWith("/") ? new String[]{"/"} : new String[]{""};

		if (path.startsWith("/") && !components[0].equals("/")) {
			final String[] prependRoot = new String[components.length + 1];
			prependRoot[0] = "/";
			System.arraycopy(components, 0, prependRoot, 1, components.length);
			components = prependRoot;
		}

		if (path.endsWith("/") && !components[components.length - 1].endsWith("/")) {
			components[components.length - 1] = components[components.length - 1] + "/";
		}
		return components;
	}

	/**
	 * Compose a path from a base uri and subsequent components.
	 *
	 * @param uri the base path uri
	 * @param components the path components
	 * @return the path
	 */
	default String compose( final URI uri, final String... components ) {

		int firstNonEmptyIdx = 0;
		while (firstNonEmptyIdx < components.length && (components[firstNonEmptyIdx] == null || components[firstNonEmptyIdx].isEmpty())) {
			firstNonEmptyIdx++;
		}

		/*If there are no non-empty components, there is nothing to compose against; return the uri. */
		if (components.length == firstNonEmptyIdx)
			return uri.toString();

		/* allocate space for the initial path and the new components, skipping empty strings  */
		final int nonEmptysize = components.length - firstNonEmptyIdx;
		final String[] allComponents = new String[1 + nonEmptysize];
		if (uri.getPath().isEmpty())
			//TODO Caleb: This `isEmpty()` check is only necessary for Java 8. In newer versions
			//	URI resolution is updated so that resolving and empty path with a new path adds
			//	a leading `/` between the rest of the URI and the path part. In Java 8 it doesn't
			//  add the `/` so it ends up directly concatenating the path part with URI
			allComponents[0] = "/";
		else
			allComponents[0] = uri.getPath();

		System.arraycopy(components, firstNonEmptyIdx, allComponents, 1, nonEmptysize);

		URI composedUri = uri;
		for (int i = 0; i < allComponents.length; i++) {
			final String component = allComponents[i];
			if (component == null || component.isEmpty())
				continue;
			else if (component.endsWith("/") || i == allComponents.length - 1)
				composedUri = composedUri.resolve(N5URI.encodeAsUriPath(component));
			else
				composedUri = composedUri.resolve(N5URI.encodeAsUriPath(component + "/"));
		}
		return composedUri.toString();
	}

	@Deprecated
	default String compose( final String... components ) {

		return normalize(
				Arrays.stream(components)
						.filter(x -> !x.isEmpty())
						.collect(Collectors.joining("/"))
		);

	}

	/**
	 * Get the parent of a path string.
	 *
	 * @param path
	 *            the path
	 * @return the parent path or null if the path has no parent
	 */
	default String parent( final String path ) {
		final String removeTrailingSlash = path.replaceAll("/+$", "");
		return normalize(N5URI.getAsUri(removeTrailingSlash).resolve("").toString());
	}

	/**
	 * Relativize path relative to base.
	 *
	 * @param path
	 *            the path
	 * @param base
	 *            the base path
	 * @return the result or null if the path has no parent
	 */
	default String relativize( final String path, final String base ) {

		try {
			/*
			 * Must pass absolute path to `uri`. if it already is, this is
			 * redundant, and has no impact on the result. It's not true that
			 * the inputs are always referencing absolute paths, but it doesn't
			 * matter in this case, since we only care about the relative
			 * portion of `path` to `base`, so the result always ignores the
			 * absolute prefix anyway.
			 */
			return normalize(uri("/" + base).relativize(uri("/" + path)).toString());
		} catch (final URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
		}
	}

	/**
	 * Normalize a path to canonical form. All paths pointing to the same
	 * location return the same output. This is most important for cached
	 * data pointing at the same location getting the same key.
	 *
	 * @param path
	 *            the path
	 * @return the normalized path
	 */
	String normalize( final String path );

	/**
	 * Get the absolute (including scheme) {@link URI} of the given path
	 *
	 * @param uriString
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return absolute URI
	 * @throws URISyntaxException if the given path is not a proper URI
	 */
	default URI uri(final String uriString) throws URISyntaxException {
		try {
			return URI.create(uriString);
		} catch (Exception ignore) {
			return N5URI.encodeAsUri(uriString);
		}
	}
	/**
	 * Test whether the path exists.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path exists
	 */
	boolean exists( final String normalPath );

	/**
	 * Returns the size in bytes of the object at the given normalPath if it exists.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the size of the object in bytes.
	 * @throws N5Exception.N5NoSuchKeyException if the given key does not exist
	 */
	long size( final String normalPath ) throws N5Exception.N5NoSuchKeyException;

	/**
	 * Test whether the path is a directory.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path is a directory
	 */
	boolean isDirectory( String normalPath );

	/**
	 * Test whether the path is a file.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return true if the path is a file
	 */
	boolean isFile( String normalPath ); // TODO: Looks un-used. Remove?

	/**
	 * Create a {@link ReadData} through which data at the normal key can be read.
	 * <p>
	 * Implementations should read lazily if possible. Consumers may call {@link ReadData#materialize()} to force
	 * a read operation if needed.
	 * <p>
	 * If supported by this KeyValueAccess implementation, partial reads are possible by calling slice on the output {@link ReadData}.
	 *
	 * @param normalPath is expected to be in normalized form, no further efforts are made to normalize it
	 * @return a materialized Read data
	 * @throws N5IOException if an error occurs
	 */
	ReadData createReadData( final String normalPath ) throws N5IOException;

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
	 * @throws N5IOException
	 *             if a locked channel could not be created
	 */
	LockedChannel lockForReading( final String normalPath ) throws N5IOException;

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
	 * @throws N5IOException
	 *             if a locked channel could not be created
	 */
	LockedChannel lockForWriting( final String normalPath ) throws N5IOException;

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the directories
	 * @throws N5IOException
	 *             if an error occurs during listing
	 */
	String[] listDirectories( final String normalPath ) throws N5IOException;

	/**
	 * List all children of a path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the the child paths
	 * @throws N5IOException if an error occurs during listing
	 */
	String[] list( final String normalPath ) throws N5IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @throws N5IOException
	 *             if an error occurs during creation
	 */
	void createDirectories( final String normalPath ) throws N5IOException;

	/**
	 * Delete a path. If the path is a directory, delete it recursively.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @throws N5IOException
	 *            if an error occurs during deletion
	 */
	void delete( final String normalPath ) throws N5IOException;

	/**
	 * A lazy reading strategy for lazy, partial reading of data from some source.
	 * <p>
	 * Implementations of this interface handle the specifics of accessing data from
	 * their respective sources.
	 * 
	 * @see ReadData
	 * @see KeyValueAccessReadData
	 */
	// TODO: Why is this in KeyValueAccess? Move it to separate class in org.janelia.saalfeldlab.n5.readdata.kva ?
	interface LazyRead extends Closeable {

		/**
		 * Materializes a portion of the data into a concrete {@link ReadData}
		 * instance.
		 * <p>
		 * This method performs the actual read operation from the underlying
		 * source, loading only the requested portion of data. The implementation
		 * should handle bounds checking and throw appropriate exceptions for
		 * invalid ranges.
		 *
		 * @param offset
		 *            the starting position in the data source
		 * @param length
		 *            the number of bytes to read, or -1 to read from offset to end
		 * @return a materialized {@link ReadData} instance containing the requested
		 *         data
		 * @throws N5IOException
		 *             if any I/O error occurs
		 */
		ReadData materialize(long offset, long length) throws N5IOException;

		/**
		 * Returns the total size of the data source in bytes.
		 * 
		 * @return the size of the data source in bytes
		 * @throws N5IOException
		 * 		if an I/O error occurs while trying to get the length
		 */
		long size() throws N5IOException;

	}

}
