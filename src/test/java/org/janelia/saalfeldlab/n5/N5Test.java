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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.N5.CompressionType;
import org.janelia.saalfeldlab.n5.N5.DataType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class N5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";

	static private String groupName = "/test/group";

	static private String datasetName = "/test/group/dataset";

	static private long[] dimensions = new long[]{100, 200, 300};

	static private int[] cellSize = new int[]{33, 22, 11};

	static private N5 n5;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		final File testDir = new File(testDirPath);
		testDir.mkdirs();
		if (!(testDir.exists() && testDir.isDirectory()))
			throw new IOException("Could not create test directory for HDF5Utils test.");

		n5 = new N5(testDirPath);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void rampDownAfterClass() throws Exception {
		final File testDir = new File(testDirPath);
		testDir.delete();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

	}

	@Test
	public void testCreateGroup() {
		try {
			n5.createGroup(groupName);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final File file = Paths.get(testDirPath, groupName).toFile();
		if (!(file.exists() && file.isDirectory()))
			fail("File does not exist");
	}
	@Test
	public void testCreateDataset() {
		try {
			n5.createDataset(datasetName, dimensions, cellSize, DataType.UINT64, CompressionType.RAW);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final File file = Paths.get(testDirPath, datasetName).toFile();
		if (!(file.exists() && file.isDirectory()))
			fail("File does not exist");

		try {
			final DatasetAttributes info = n5.getDatasetInfo(datasetName);
			Assert.assertArrayEquals(dimensions, info.getDimensions());
			Assert.assertArrayEquals(cellSize, info.getBlockSize());
			Assert.assertEquals(DataType.UINT64, info.getDataType());
			Assert.assertEquals(CompressionType.RAW, info.getCompressionType());
		} catch (final IOException e) {
			fail("Dataset info cannot be opened");
			e.printStackTrace();
		}
	}

	@Test
	public void testRemove() {
		try {
			n5.remove(groupName);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final File file = Paths.get(testDirPath, groupName).toFile();
		if (file.exists())
			fail("Group still exists not exist");
	}
}
