package org.janelia.saalfeldlab.n5;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractN5VersionedTest extends AbstractN5Test {

	@Override
	protected abstract N5VersionedWriter createN5Writer() throws IOException;

	@Test
	public void testVersion() throws NumberFormatException, IOException {

		final N5VersionedWriter n5Versioned = (N5VersionedWriter)n5;
		n5Versioned.checkVersion();

		Assert.assertEquals(N5VersionedReader.VERSION, n5Versioned.getVersionString());

		Assert.assertArrayEquals(
				new int[] {
						N5VersionedReader.VERSION_MAJOR,
						N5VersionedReader.VERSION_MINOR,
						N5VersionedReader.VERSION_PATCH
					},
				n5Versioned.getVersion()
			);
	}
}
