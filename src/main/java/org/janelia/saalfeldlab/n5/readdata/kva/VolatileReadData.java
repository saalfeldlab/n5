package org.janelia.saalfeldlab.n5.readdata.kva;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

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
}
