package org.janelia.saalfeldlab.n5;


import java.net.URI;
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
	 * @throws N5Exception.N5IOException
	 * 		if an error occurs
	 */
	VolatileReadData createReadData(final URI normalPath) throws N5Exception.N5IOException;

	// TODO: Do we want this, or should we just rely on URI everywhere?
	default VolatileReadData createReadData(final String normalPath) throws N5Exception.N5IOException {
		return createReadData(URI.create(normalPath));
	}

}
