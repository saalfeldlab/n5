package org.janelia.saalfeldlab.n5;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class FileSystemRootedKeyValueAccess implements RootedKeyValueAccess {

	private final URI root;

	public FileSystemRootedKeyValueAccess(final String basePath) throws N5IOException {
		this.root = URI.create(basePath);
	}


	@Override
	public VolatileReadData createReadData(final URI normalPath) throws N5IOException {

		try {
			return _read(root.resolve(normalPath));
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for reading: " + normalPath, e);
		}

	}

	@Override
	public boolean isDirectory(final URI normalPath) {

		final Path path = Path.of(root.resolve(normalPath));
		return Files.isDirectory(path);
	}

	@Override
	public boolean isFile(final URI normalPath) {

		final Path path = Path.of(root.resolve(normalPath));
		return Files.isRegularFile(path);
	}

	@Override
	public void write(final URI normalPath, final ReadData data) throws N5IOException {

		try {
			_write(root.resolve(normalPath), data);
		} catch (final NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to lock file for writing: " + normalPath, e);
		}
	}

	@Override
	public void createDirectories(final URI normalPath) throws N5IOException {

		try {
			createDirectories(Path.of(root.resolve(normalPath)));
		} catch (NoSuchFileException e) {
			throw new N5NoSuchKeyException("No such file", e);
		} catch (IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to create directories", e);
		}
	}

	@Override
	public void delete(final URI normalPath) throws N5IOException {

		try {
			final Path path = Path.of(root.resolve(normalPath));

			if (Files.isRegularFile(path))
				ioPolicy.delete(path.toString());
			else {
				try (final Stream<Path> pathStream = Files.walk(path)) {
					for (final Iterator<Path> i = pathStream.sorted(Comparator.reverseOrder()).iterator(); i.hasNext(); ) {
						final Path childPath = i.next();
						if (Files.isRegularFile(childPath))
							ioPolicy.delete(childPath.toString());
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




	//
 	// ------------------------------------------------------------------------
 	//
	// -- forward to existing IoPolicy interface --
	//    (absolute paths, as strings. maybe revise later...)


	private VolatileReadData _read(final URI uri) throws IOException {

		return ioPolicy.read(new File(uri).getAbsolutePath());
	}

	private void _write(final URI uri, final ReadData data) throws IOException {

		ioPolicy.write(new File(uri).getAbsolutePath(), data);
	}

	private final IoPolicy ioPolicy = new FsIoPolicy.Atomic();;




	//
	// ------------------------------------------------------------------------
	//
	// -- helper methods copied from FileSystemKeyValueAccess --
	//

	// TODO: is protected static in KVA, but we don't have static ioPolicy field here
	private void tryDelete(final Path path) throws IOException {

		try {
			ioPolicy.delete(path.toString());
		} catch (final DirectoryNotEmptyException e) {
			/*
			 * Even though path is expected to be an empty directory, sometimes
			 * deletion fails on network filesystems when lock files are not
			 * cleared immediately after the leaves have been removed.
			 */
			try {
				/* wait and reattempt */
				Thread.sleep(100);
				ioPolicy.delete(path.toString());
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

}
