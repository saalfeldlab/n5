package org.janelia.saalfeldlab.n5;


import java.net.URI;
import java.nio.file.FileSystem;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * Key value read/write primitives used by {@link GsonKeyValueN5Reader}, {@link
 * GsonKeyValueN5Writer}, and {@link KeyValueRootHierarchyStore}. This interface
 * implements a subset of access primitives provided by {@link FileSystem} to
 * reduce the implementation burden for backends lacking a {@link FileSystem}
 * implementation (such as AWS-S3).
 * <p>
 * TODO: This is intended to eventually fully replace {@link KeyValueAccess}.
 */
public interface KeyValueRoot {

	@Deprecated
	KeyValueAccess getKVA();

	/**
	 * Returns the absolute URI of this root.
	 *
	 * @return the base path URI
	 */
	URI uri();

	/**
	 * Create a {@link VolatileReadData} through which data at the normal key
	 * can be read.
	 * <p>
	 * Implementations should read lazily if possible. Consumers may call {@link
	 * ReadData#materialize()} to force a read operation if needed.
	 * <p>
	 * If supported by this KeyValueAccess implementation, partial reads are
	 * possible by {@link ReadData#slice slicing} the returned {@code ReadData}.
	 * <p>
	 * The resulting {@code VolatileReadData} is potentially lazy. If the requested
	 * key does not exist, it will throw {@code N5NoSuchKeyException}. Whether
	 * the exception is thrown when {@link #createReadData} is called,
	 * or when trying to materialize the {@code VolatileReadData} is implementation dependent.
	 *
	 * @param normalPath
	 * 		path to read, relative to container root
	 *
	 * @return a ReadData
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
//	TODO: rename to just "read" ??
	VolatileReadData createReadData(N5FilePath normalPath) throws N5IOException;

	/**
	 * Test whether the path exists and is a directory.
	 *
	 * @param normalPath
	 * 		path to test, relative to container root
	 *
	 * @return true if the path is a directory
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	boolean isDirectory(N5Path normalPath) throws N5IOException;

	/**
	 * Test whether the path exists and is a file.
	 *
	 * @param normalPath
	 * 		path to test, relative to container root
	 *
	 * @return true if the path is a file
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	boolean isFile(N5Path normalPath) throws N5IOException;

	/**
	 * Test whether the path exists.
	 *
	 * @param normalPath
	 * 		path to test, relative to container root
	 *
	 * @return true if the path exists
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	boolean exists(N5Path normalPath) throws N5IOException;

	/**
	 * Returns the size in bytes of the object at the given normalPath if it exists.
	 *
	 * @param normalPath
	 * 		path to query, relative to container root
	 *
	 * @return the size of the object in bytes.
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	long size(N5FilePath normalPath) throws N5IOException;

	/**
	 * Write {@code data} to the given {@code normalPath}.
	 * <p>
	 * Existing data at {@code normalPath} will be overridden.
	 *
	 * @param normalPath
	 * 		path to write, relative to container root
	 * @param data
	 * 		the data to write
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	void write(N5FilePath normalPath, ReadData data) throws N5IOException;

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath
	 * 		path to list, relative to container root
	 *
	 * @return the directories (relative to {@code normalPath})
	 *
	 * @throws N5NoSuchKeyException
	 * 		if the given path does not exist
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 */
	String[] listDirectories(N5DirectoryPath normalPath) throws N5IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param normalPath
	 * 		directory path to create, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during creation
	 */
	void createDirectories(N5DirectoryPath normalPath) throws N5IOException;

	/**
	 * Delete a path. If the path is a directory, delete it recursively.
	 *
	 * @param normalPath
	 * 		path to delete, relative to container root
	 *
	 * @throws N5IOException
	 * 		if an error occurs during deletion
	 */
	void delete(N5Path normalPath) throws N5IOException;

	// ----------------------------------------------------------------

	default VolatileReadData createReadData(final String normalPath) throws N5IOException {
		return createReadData(N5FilePath.of(normalPath));
	}

	default boolean isDirectory(final String normalPath) {
		return isDirectory(N5Path.of(normalPath));
	}

	default boolean isFile(final String normalPath) {
		return isFile(N5Path.of(normalPath));
	}

	default boolean exists(final String normalPath) {
		return exists(N5Path.of(normalPath));
	}

	default long size(final String normalPath) throws N5IOException {
		return size(N5FilePath.of(normalPath));
	}

	default void write(final String normalPath, final ReadData data) throws N5IOException {
		write(N5FilePath.of(normalPath), data);
	}

	default String[] listDirectories(final String normalPath) throws N5IOException {
		return listDirectories(N5DirectoryPath.of(normalPath));
	}

	default void createDirectories( final String normalPath ) throws N5IOException {
		createDirectories(N5DirectoryPath.of(normalPath));
	}

	default void delete(final String normalPath) throws N5IOException {
		delete(N5Path.of(normalPath));
	}
}
