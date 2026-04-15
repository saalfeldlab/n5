package org.janelia.saalfeldlab.n5;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class RootedFileSystemKeyValueAccess implements RootedKeyValueAccess {

	private final URI root;
	private final FileKeyLockManager fileKeyLockManager;

	// TODO: Turning basePath String into root URI here is fragile. It might already be a URI ("file:/...") or just a path, it might be relative or absolute, etc...
	//       Maybe it would be better to take a URI here and handle the String higher up (where we might have more information)?
	public RootedFileSystemKeyValueAccess(final String basePath) throws N5IOException {

		// NB: We want to make sure that the root URI is a directory, that is,
		// it ends with a slash. (Otherwise, relativizing and resolution against
		// the root URI will not work correctly.)

		// First we turn basePath into a URI. This takes care of OS specific
		// separators and escaping of special characters:
		final URI uri = uriForNormalPath(basePath);

		// However, the resulting URI may not end in a slash in which case we
		// append one.
		final String uriStr = uri.toString();
		this.root = uriStr.endsWith("/") ? uri : URI.create(uriStr + "/");

		final LockingPolicy policy = LockingPolicy.fromString(System.getProperty("n5.ioPolicy", "permissive"));
		this.fileKeyLockManager = new FileKeyLockManager(policy);
	}

	private static URI uriForNormalPath(final String normalPath) {
		// normalize make absolute the scheme specific part only
		try {
			final URI normalUri = URI.create(normalPath);
			if (normalUri.isAbsolute()) return normalUri.normalize();
		} catch (final IllegalArgumentException e) {
			return new File(normalPath).toURI().normalize();
		}
		return new File(normalPath).toURI().normalize();
	}

	@Deprecated
	@Override
	public KeyValueAccess getKVA() {
		return kva;
	}
	private final KeyValueAccess kva = new FileSystemKeyValueAccess();

	@Override
	public URI root() {
		return root;
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) throws N5IOException {
		return VolatileReadData.from(new FileLazyRead(resolve(normalPath)));
	}

	@Override
	public boolean isDirectory(final N5Path normalPath) {
		return Files.isDirectory(resolve(normalPath));
	}

	@Override
	public boolean isFile(final N5Path normalPath) {
		return Files.isRegularFile(resolve(normalPath));
	}

	@Override
	public boolean exists(final N5Path normalPath) {
		return Files.exists(resolve(normalPath));
	}

	@Override
	public long size(final N5FilePath normalPath) throws N5IOException {

		try {
			return Files.size(resolve(normalPath));
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public void write(final N5FilePath normalPath, final ReadData data) throws N5IOException {

		try (final LockedFileChannel channel = lockForWriting(resolve(normalPath))) {
			data.writeTo(channel.newOutputStream());
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath normalPath) throws N5IOException {

		final Path path = resolve(normalPath);
		try (final Stream<Path> pathStream = Files.list(path)) {
			return pathStream
					.filter(Files::isDirectory)
					.map(a -> path.relativize(a).toString())
					.toArray(String[]::new);
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to list directories", e);
		}
	}

	@Override
	public void createDirectories(final N5DirectoryPath normalPath) throws N5IOException {

		try {
			createDirectories(resolve(normalPath));
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to create directories", e);
		}
	}

	@Override
	public void delete(final N5Path normalPath) throws N5IOException {

		try {
			final Path path = resolve(normalPath);

			if (Files.isRegularFile(path))
				try (final LockedChannel channel = lockForWriting(path)) {
					Files.delete(path);
				}
			else {
				try (final Stream<Path> pathStream = Files.walk(path)) {
					for (final Iterator<Path> i = pathStream.sorted(Comparator.reverseOrder()).iterator(); i.hasNext(); ) {
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
		} catch (NoSuchFileException ignore) {
			/* It doesn't exist; that's sufficient for us to not complain on a `delete` call */
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to delete file at " + normalPath, e);
		}
	}

 	// ------------------------------------------------------------------------
 	//
	// -- helper methods --
	//

	/**
	 * Resolve a relative {@code path} (relative with respect to the container
	 * {@link #root}) to an absolute {@link Path}.
	 *
	 * @param normalPath path to resolve relative to container root
	 * @return resolved absolute path
	 */
	private Path resolve(final N5Path normalPath) {
		return Path.of(root.resolve(normalPath.uri()));
	}

	private LockedFileChannel lockForReading(final Path path) throws N5IOException {

		try {
			return fileKeyLockManager.lockForReading(path);
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for reading: " + path, e);
		}
	}

	private LockedFileChannel lockForWriting(final Path path) throws N5IOException {

		try {
			return fileKeyLockManager.lockForWriting(path);
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for writing: " + path, e);
		}
	}

	private static void tryDelete(final Path path) throws IOException {

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
	private static Path createDirectories(Path dir, final FileAttribute<?>... attrs) throws IOException {

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
	private static void createAndCheckIsDirectory(
			final Path dir,
			final FileAttribute<?>... attrs) throws IOException {

		try {
			Files.createDirectory(dir, attrs);
		} catch (final FileAlreadyExistsException x) {
			if (!Files.isDirectory(dir))
				throw x;
		}
	}

	private static boolean validBounds(long channelSize, long offset, long length) {

		if (offset < 0)
			return false;
		else if (channelSize > 0 && offset >= channelSize) // offset == 0 and channelSize == 0 is okay
			return false;
		else if (length >= 0 && offset + length > channelSize)
			return false;

		return true;
	}

	private class FileLazyRead implements LazyRead {

		private final Path path;
		private LockedFileChannel lock;

		FileLazyRead(final Path path) {
			this.path = path;
			lock = lockForReading(path);
		}

		@Override
		public long size() throws N5IOException {

			if (lock == null) {
				throw new N5IOException("FileLazyRead is already closed.");
			}

			try {
				return Files.size(path);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		@Override
		public ReadData materialize(final long offset, final long length) {

			if (lock == null) {
				throw new N5IOException("FileLazyRead is already closed.");
			}

			try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

				channel.position(offset);

				final long channelSize = channel.size();
				if (!validBounds(channelSize, offset, length)) {
					throw new IndexOutOfBoundsException();
				}

				final long size = length < 0 ? (channelSize - offset) : length;
				if (size > Integer.MAX_VALUE) {
					throw new IndexOutOfBoundsException("Attempt to materialize too large data");
				}

				final byte[] data = new byte[(int) size];
				final ByteBuffer buf = ByteBuffer.wrap(data);
				channel.read(buf);
				return ReadData.from(data);

			} catch (final NoSuchFileException e) {
				throw new N5NoSuchKeyException("No such file", e);
			} catch (IOException | UncheckedIOException e) {
				throw new N5IOException(e);
			}
		}

		@Override
		public void close() throws IOException {

			if (lock != null) {
				lock.close();
				lock = null;
			}
		}
	}
}
