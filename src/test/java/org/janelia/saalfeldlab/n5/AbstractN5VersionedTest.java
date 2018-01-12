/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Abstract base class for testing versioned N5 functionality.
 * Subclasses are expected to provide a specific N5 implementation to be tested by defining the {@link #createN5Writer()} method.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
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
