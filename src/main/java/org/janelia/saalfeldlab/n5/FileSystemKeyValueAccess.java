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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.readdata.ReadData;

import static org.janelia.saalfeldlab.n5.FileKeyLockManager.FILE_LOCK_MANAGER;

/**
 * Filesystem {@link KeyValueAccess}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 * @author Philipp Hanslovsky
 */
public class FileSystemKeyValueAccess implements KeyValueAccess {

	protected final FileSystem fileSystem;

	/**
	 * Opens a {@link FileSystemKeyValueAccess} with a {@link FileSystem}.
	 *
	 * @param fileSystem the file system
	 */
	public FileSystemKeyValueAccess(final FileSystem fileSystem) {

		this.fileSystem = fileSystem;
	}

	@Override
	public ReadData createReadData(final String normalPath) {
		return new KeyValueAccessReadData(new FileLazyRead(normalPath));
	}

	@Override
	public LockedFileChannel lockForReading(final String normalPath) throws N5IOException {

		try {
			return FILE_LOCK_MANAGER.lockForReading(fileSystem.getPath(normalPath));
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for reading: " + normalPath, e);
		}
	}

	@Override
	public LockedChannel lockForWriting(final String normalPath) throws N5IOException {

		try {
			return FILE_LOCK_MANAGER.lockForWriting(fileSystem.getPath(normalPath));
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for writing: " + normalPath, e);
		}
	}

	public LockedFileChannel lockForReading(final Path path) throws N5IOException {

		try {
			return FILE_LOCK_MANAGER.lockForReading(path);
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for reading: " + path, e);
		}
	}

	public LockedFileChannel lockForWriting(final Path path) throws N5IOException {

		try {
			return FILE_LOCK_MANAGER.lockForWriting(path);
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for writing: " + path, e);
		}
	}

	@Override
	public boolean isDirectory(final String normalPath) {

		final Path path = fileSystem.getPath(normalPath);
		return Files.isDirectory(path);
	}

	@Override
	public boolean isFile(final String normalPath) {

		final Path path = fileSystem.getPath(normalPath);
		return Files.isRegularFile(path);
	}

	@Override
	public boolean exists(final String normalPath) {

		final Path path = fileSystem.getPath(normalPath);
		return Files.exists(path);
	}

	@Override
	public long size(final String normalPath) {

		try {
			return Files.size(fileSystem.getPath(normalPath));
		} catch (NoSuchFileException e) {
			throw new N5Exception.N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5Exception.N5IOException(e);
		}
	}

	@Override
	public String[] listDirectories(final String normalPath) throws N5IOException {

		final Path path = fileSystem.getPath(normalPath);
		try (final Stream<Path> pathStream = Files.list(path)) {
			return pathStream
					.filter(a -> Files.isDirectory(a))
					.map(a -> path.relativize(a).toString())
					.toArray(n -> new String[n]);
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to list directories", e);
		}
	}

	@Override
	public String[] list(final String normalPath) throws N5IOException {

		final Path path = fileSystem.getPath(normalPath);
		try (final Stream<Path> pathStream = Files.list(path)) {
			return pathStream
					.map(a -> path.relativize(a).toString())
					.toArray(n -> new String[n]);
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to list files", e);
		}
	}

	@Override
	public String[] components(final String path) {

		final Path fsPath = fileSystem.getPath(path);
		final Path root = fsPath.getRoot();
		final String[] components;
		int o;
		if (root == null) {
			components = new String[fsPath.getNameCount()];
			o = 0;
		} else {
			components = new String[fsPath.getNameCount() + 1];
			components[0] = root.toString();
			o = 1;
		}

		for (int i = o; i < components.length; ++i) {
			String name = fsPath.getName(i - o).toString();
			/* Preserve trailing slash on final component if present*/
			if (i == components.length - 1) {
				final String separator = fileSystem.getSeparator();
				final String trailingSeparator = path.endsWith(separator) ? separator : path.endsWith("/") ? "/" : "";
				name += trailingSeparator;
			}
			components[i] = name;
		}
		return components;
	}

	@Override
	public String parent(final String path) {

		final Path parent = fileSystem.getPath(path).getParent();
		if (parent == null)
			return null;
		else
			return parent.toString();
	}

	@Override
	public String relativize(final String path, final String base) {

		final Path basePath = fileSystem.getPath(base);
		return basePath.relativize(fileSystem.getPath(path)).toString();
	}

	/**
	 * Returns a normalized path. It ensures correctness on both Unix and
	 * Windows,
	 * otherwise {@code pathName} is treated as UNC path on Windows, and
	 * {@code Paths.get(pathName, ...)} fails with {@code InvalidPathException}.
	 *
	 * @param path the path
	 * @return the normalized path, without leading slash
	 */
	@Override
	public String normalize(final String path) {

		return fileSystem.getPath(path).normalize().toString();
	}

	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		// normalize make absolute the scheme specific part only
		try {
			final URI normalUri = URI.create(normalPath);
			if (normalUri.isAbsolute()) return normalUri.normalize();
		} catch (final IllegalArgumentException e) {
			return new File(normalPath).toURI().normalize();
		}
		return new File(normalPath).toURI().normalize();

	}

	@Override
	public String compose(final String... components) {

		if (components == null || components.length == 0)
			return null;
		if (components.length == 1)
			return fileSystem.getPath(components[0]).toString();
		return fileSystem.getPath(components[0], Arrays.copyOfRange(components, 1, components.length)).normalize().toString();
	}

	@Override public String compose(URI uri, String... components) {

		Path composedPath;
		if (uri.isAbsolute())
			composedPath = Paths.get(uri);
		else
			composedPath = Paths.get(uri.toString());
		for (String component : components) {
			if (component == null || component.isEmpty())
				continue;
			composedPath = composedPath.resolve(component);
		}

		return composedPath.toAbsolutePath().toString();
	}

	@Override
	public void createDirectories(final String normalPath) throws N5IOException {

		try {
			createDirectories(fileSystem.getPath(normalPath));
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to create directories", e);
		}
	}

	@Override
	public void delete(final String normalPath) throws N5IOException {

		try {
			final Path path = fileSystem.getPath(normalPath);

			if (Files.isRegularFile(path))
				try (final LockedChannel channel = lockForWriting(path)) {
					Files.delete(path);
				}
			else {
				try (final Stream<Path> pathStream = Files.walk(path)) {
					for (final Iterator<Path> i = pathStream.sorted(Comparator.reverseOrder()).iterator(); i.hasNext();) {
						final Path childPath = i.next();
						if (Files.isRegularFile(childPath))
							try (final LockedChannel channel = lockForWriting(childPath)) {
								Files.delete(childPath);
							}
						else
							tryDelete(childPath);
					}
				}
			}
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to delete file at " + normalPath, e);
		}
	}

	protected static void tryDelete(final Path path) throws IOException {

		try {
			Files.delete(path);
		} catch (final DirectoryNotEmptyException e) {
			/*
			 * Even though path is expected to be an empty directory, sometimes
			 * deletion fails on network filesystems when lock files are not
			 * cleared immediately after the leaves have been removed.
			 */
			try {
				/* wait and reattempt */
				Thread.sleep(100);
				Files.delete(path);
			} catch (final InterruptedException ex) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * This is a copy of {@link Files#createDirectories(Path, FileAttribute...)}
	 * that follows symlinks.
	 *
	 * Workaround for https://bugs.openjdk.java.net/browse/JDK-8130464
	 *
	 * Creates a directory by creating all nonexistent parent directories first.
	 * Unlike the {@link Files#createDirectories} method, an exception
	 * is not thrown if the directory could not be created because it already
	 * exists.
	 *
	 * <p>
	 * The {@code attrs} parameter is optional {@link FileAttribute
	 * file-attributes} to set atomically when creating the nonexistent
	 * directories. Each file attribute is identified by its {@link
	 * FileAttribute#name name}. If more than one attribute of the same name is
	 * included in the array then all but the last occurrence is ignored.
	 *
	 * <p>
	 * If this method fails, then it may do so after creating some, but not
	 * all, of the parent directories.
	 *
	 * @param dir
	 *            the directory to create
	 *
	 * @param attrs
	 *            an optional list of file attributes to set atomically when
	 *            creating the directory
	 *
	 * @return the directory
	 *
	 * @throws UnsupportedOperationException
	 *             if the array contains an attribute that cannot be set
	 *             atomically
	 *             when creating the directory
	 * @throws FileAlreadyExistsException
	 *             if {@code dir} exists but is not a directory <i>(optional
	 *             specific
	 *             exception)</i>
	 * @throws IOException
	 *             if an I/O error occurs
	 * @throws SecurityException
	 *             in the case of the default provider, and a security manager
	 *             is
	 *             installed, the {@link SecurityManager#checkWrite(String)
	 *             checkWrite}
	 *             method is invoked prior to attempting to create a directory
	 *             and
	 *             its {@link SecurityManager#checkRead(String) checkRead} is
	 *             invoked for each parent directory that is checked. If {@code
	 *          dir} is not an absolute path then its {@link Path#toAbsolutePath
	 *             toAbsolutePath} may need to be invoked to get its absolute
	 *             path.
	 *             This may invoke the security manager's {@link
	 *             SecurityManager#checkPropertyAccess(String)
	 *             checkPropertyAccess}
	 *             method to check access to the system property
	 *             {@code user.dir}
	 */
	protected static Path createDirectories(Path dir, final FileAttribute<?>... attrs) throws IOException {

		// attempt to create the directory
		try {
			createAndCheckIsDirectory(dir, attrs);
			return dir;
		} catch (final FileAlreadyExistsException x) {
			// file exists and is not a directory
			throw x;
		} catch (final IOException x) {
			// parent may not exist or other reason
		}
		SecurityException se = null;
		try {
			dir = dir.toAbsolutePath();
		} catch (final SecurityException x) {
			// don't have permission to get absolute path
			se = x;
		}
		// find a descendant that exists
		Path parent = dir.getParent();
		while (parent != null) {
			try {
				parent.getFileSystem().provider().checkAccess(parent);
				break;
			} catch (final NoSuchFileException x) {
				// does not exist
			}
			parent = parent.getParent();
		}
		if (parent == null) {
			// unable to find existing parent
			if (se == null) {
				throw new FileSystemException(
						dir.toString(),
						null,
						"Unable to determine if root directory exists");
			} else {
				throw se;
			}
		}

		// create directories
		Path child = parent;
		for (final Path name : parent.relativize(dir)) {
			child = child.resolve(name);
			createAndCheckIsDirectory(child, attrs);
		}
		return dir;
	}

	/**
	 * This is a copy of a previous Files#createAndCheckIsDirectory(Path,
	 * FileAttribute...) method that follows symlinks.
	 *
	 * Workaround for https://bugs.openjdk.java.net/browse/JDK-8130464
	 *
	 * Used by createDirectories to attempt to create a directory. A no-op if the
	 * directory already exists.
	 *
	 * @param dir directory path
	 * @param attrs file attributes
	 * @throws IOException the exception
	 */
	protected static void createAndCheckIsDirectory(
			final Path dir,
			final FileAttribute<?>... attrs) throws IOException {

		try {
			Files.createDirectory(dir, attrs);
		} catch (final FileAlreadyExistsException x) {
			if (!Files.isDirectory(dir))
				throw x;
		}
	}

	private class FileLazyRead implements LazyRead {

		private final String normalKey;

	    FileLazyRead(String normalKey) {
	        this.normalKey = normalKey;
	    }

	    @Override
	    public long size() {
	        return FileSystemKeyValueAccess.this.size(normalKey);
	    }

	    @Override
	    public ReadData materialize(final long offset, final long length) {

	        try (final LockedFileChannel lfs = lockForReading(fileSystem.getPath(normalKey))) {
	            final FileChannel channel = lfs.getFileChannel();
	            channel.position(offset);
	            if (length > Integer.MAX_VALUE)
	                throw new IOException("Attempt to materialize too large data");

	            final long channelSize = channel.size();
	            if (!validBounds(channelSize, offset, length))
	                throw new IndexOutOfBoundsException();

	            final int sz = (int) (length < 0 ? channelSize : length);
	            final byte[] data = new byte[sz];
	            final ByteBuffer buf = ByteBuffer.wrap(data);
	            channel.read(buf);
	            return ReadData.from(data);

	        } catch (final NoSuchFileException e) {
	            throw new N5NoSuchKeyException("No such file", e);
	        } catch (IOException | UncheckedIOException e) {
	            throw new N5Exception.N5IOException(e);
	        }
	    }

		@Override
		public void close() throws IOException {
			// TODO: implement FileLazyRead.close()
			throw new UnsupportedOperationException("TODO: implement FileLazyRead.close()");

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
