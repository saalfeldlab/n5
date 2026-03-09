package org.janelia.saalfeldlab.n5;


import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * TODO: This is intented to eventually replace KeyValueAccess. The plan is to
 *   incrementally replace usages of KeyValueAccess, adding methods here as
 *   needed.
 */
public interface RootedKeyValueAccess {

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
	 * the exception is thrown when {@link KeyValueAccess#createReadData(String)}] is called,
	 * or when trying to materialize the {@code VolatileReadData} is implementation dependent.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it
	 *
	 * @return a ReadData
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	VolatileReadData createReadData(URI normalPath) throws N5IOException;

	/**
	 * Test whether the path exists and is a directory.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @return true if the path is a directory
	 */
	boolean isDirectory(URI normalPath);

	/**
	 * Test whether the path exists and is a file.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @return true if the path is a file
	 */
	boolean isFile(URI normalPath);

	/**
	 * Write {@code data} to the given {@code normalPath}.
	 * <p>
	 * Existing data at {@code normalPath} will be overridden.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it
	 * @param data
	 * 		the data to write
	 *
	 * @throws N5IOException
	 * 		if an error occurs
	 */
	void write(URI normalPath, ReadData data) throws N5IOException;

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @return the directories (relative to {@code normalPath})
	 *
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 */
	// TODO should this return URI[]
	String[] listDirectories(URI normalPath) throws N5IOException;

	/**
	 * Create a directory and all parent paths along the way. The directory
	 * and parent paths are discoverable. On a filesystem, this usually means
	 * that the directories exist, on a key value store that is unaware of
	 * directories, this may be implemented as creating an object for each path.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @throws N5IOException
	 * 		if an error occurs during creation
	 */
	void createDirectories(URI normalPath) throws N5IOException;

	/**
	 * Delete a path. If the path is a directory, delete it recursively.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @throws N5IOException
	 * 		if an error occurs during deletion
	 */
	void delete(URI normalPath) throws N5IOException;


	// ----------------------------------------------------------------
	// TODO: Where should these go? Maybe we don't need them if we rely on URI?

	static String compose(final String normalPath, final String key) {
		return normalPath.isEmpty() ? key : normalPath + "/" + key;
	}






	// ----------------------------------------------------------------
	// TODO: Do we want these, or should we just rely on URI everywhere?

	default VolatileReadData createReadData(final String normalPath) throws N5IOException {
		return createReadData(createURI(normalPath));
	}

	default boolean isDirectory(final String normalPath) {
		return isDirectory(createURI(normalPath));
	}

	default boolean isFile(final String normalPath) {
		return isFile(createURI(normalPath));
	}

	default void write(final String normalPath, final ReadData data) throws N5IOException {
		write(createURI(normalPath), data);
	}

	default String[] listDirectories(final String normalPath) throws N5IOException {
		return listDirectories(createURI(normalPath));
	}

	default void createDirectories( final String normalPath ) throws N5IOException {
		createDirectories(createURI(normalPath));
	}

	default void delete(final String normalPath) throws N5IOException {
		delete(createURI(normalPath));
	}

	private static URI createURI(final String normalPath) throws N5IOException {
		try {
			return new URI(null, null, normalPath, null);
		} catch (URISyntaxException e) {
			// This should be unreachable: Scheme/authority/fragment are null
			// and the path component accepts virtually anything
			throw new N5IOException(e);
		}
	}
}
