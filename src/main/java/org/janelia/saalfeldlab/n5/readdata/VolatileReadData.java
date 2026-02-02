package org.janelia.saalfeldlab.n5.readdata;

import java.io.InputStream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * During its life-time, the content of a {@code VolatileReadData} should not be
 * mutated.
 * <p>
 * Implementations can enforce this by
 * <ul>
 * <li>locking underlying resources until the {@code VolatileReadData} is {@link #close() closed}), or</li>
 * <li>failing with {@link N5IOException} if such modifications are detected.</li>
 * </ul>
 */
public interface VolatileReadData extends ReadData, AutoCloseable {

	@Override
	void close() throws N5IOException;

	/**
	 * Create a new {@code VolatileReadData} that loads (lazily) from {@code lazyRead}.
	 * <p>
	 * The returned {@code VolatileReadData} is responsible for {@link LazyRead#close() closing}
	 *
	 * @param lazyRead
	 * 		provides data
	 *
	 * @return a new VolatileReadData
	 */
	static VolatileReadData from(final LazyRead lazyRead) {
		return new LazyReadData(lazyRead);
	}

}
