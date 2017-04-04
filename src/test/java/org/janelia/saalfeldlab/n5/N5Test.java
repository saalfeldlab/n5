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
import java.util.Random;

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

	static private int[] blockSize = new int[]{33, 22, 11};

	static byte[] block;

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

		final Random rnd = new Random();
		block = new byte[blockSize[0] * blockSize[1] * blockSize[2]];
		rnd.nextBytes(block);
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
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, CompressionType.RAW);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final File file = Paths.get(testDirPath, datasetName).toFile();
		if (!(file.exists() && file.isDirectory()))
			fail("File does not exist");

		try {
			final DatasetAttributes info = n5.getDatasetAttributes(datasetName);
			Assert.assertArrayEquals(dimensions, info.getDimensions());
			Assert.assertArrayEquals(blockSize, info.getBlockSize());
			Assert.assertEquals(DataType.UINT64, info.getDataType());
			Assert.assertEquals(CompressionType.RAW, info.getCompressionType());
		} catch (final IOException e) {
			fail("Dataset info cannot be opened");
			e.printStackTrace();
		}
	}

	@Test
	public void testWriteReadBlock() {
		for (final CompressionType compressionType : CompressionType.values()) {
			System.out.println("Testing " + compressionType);
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT8, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(blockSize, new long[]{0, 0, 0}, block);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final AbstractDataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

				Assert.assertArrayEquals(block, (byte[])loadedDataBlock.getData());

//				Assert.assertTrue(n5.remove(datasetName));

			} catch (final IOException e) {
				e.printStackTrace();
				fail("Block cannot be written.");
			}
		}
	}

	@Test
	public void testRemove() {
		try {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, CompressionType.RAW);
			n5.remove(groupName);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final File file = Paths.get(testDirPath, groupName).toFile();
		if (file.exists())
			fail("Group still exists not exist");
	}
}
