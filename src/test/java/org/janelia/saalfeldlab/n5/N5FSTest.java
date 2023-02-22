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

import org.janelia.saalfeldlab.n5.url.UrlAttributeTest;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

/**
 * Initiates testing of the filesystem-based N5 implementation.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5FSTest extends AbstractN5Test {

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {
		String testDirPath;
		try {
			testDirPath = Files.createTempDirectory("n5-test-").toFile().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return createN5Writer(testDirPath);
	}

	@Override
	protected N5Writer createN5Writer(String location) throws IOException {
		return new N5FSWriter(location);
	}

	@Override
	protected N5Reader createN5Reader(String location) throws IOException {
		return new N5FSReader(location);
	}

	@Test
	public void customObjectTest() {

		final String testGroup = "test";
		final ArrayList<TestData<?>> existingTests = new ArrayList<>();

		final UrlAttributeTest.TestDoubles doubles1 = new UrlAttributeTest.TestDoubles("doubles", "doubles1", new double[]{5.7, 4.5, 3.4});
		final UrlAttributeTest.TestDoubles doubles2 = new UrlAttributeTest.TestDoubles("doubles", "doubles2", new double[]{5.8, 4.6, 3.5});
		final UrlAttributeTest.TestDoubles doubles3 = new UrlAttributeTest.TestDoubles("doubles", "doubles3", new double[]{5.9, 4.7, 3.6});
		final UrlAttributeTest.TestDoubles doubles4 = new UrlAttributeTest.TestDoubles("doubles", "doubles4", new double[]{5.10, 4.8, 3.7});
		addAndTest(existingTests, new TestData<>(testGroup, "/doubles[1]", doubles1));
		addAndTest(existingTests, new TestData<>(testGroup, "/doubles[2]", doubles2));
		addAndTest(existingTests, new TestData<>(testGroup, "/doubles[3]", doubles3));
		addAndTest(existingTests, new TestData<>(testGroup, "/doubles[4]", doubles4));

		/* Test overwrite custom */
		addAndTest(existingTests, new TestData<>(testGroup, "/doubles[1]", doubles4));
	}

}