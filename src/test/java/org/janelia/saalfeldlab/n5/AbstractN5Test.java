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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Abstract base class for testing N5 functionality.
 * Subclasses are expected to provide a specific N5 implementation to be tested by defining a custom {@link #setUpBeforeClass()} method.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5Test {

	static private final String groupName = "/test/group";
	static private final String[] subGroupNames = new String[]{"a", "b", "c"};
	static private final String datasetName = "/test/group/dataset";
	static private final long[] dimensions = new long[]{100, 200, 300};
	static private final int[] blockSize = new int[]{44, 33, 22};

	static private byte[] byteBlock;
	static private short[] shortBlock;
	static private int[] intBlock;
	static private long[] longBlock;
	static private float[] floatBlock;
	static private double[] doubleBlock;

	protected static N5Writer n5;
	protected static GsonAttributesParser n5Parser;

	/**
	 * @throws IOException
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {

		final Random rnd = new Random();
		byteBlock = new byte[blockSize[0] * blockSize[1] * blockSize[2]];
		shortBlock = new short[blockSize[0] * blockSize[1] * blockSize[2]];
		intBlock = new int[blockSize[0] * blockSize[1] * blockSize[2]];
		longBlock = new long[blockSize[0] * blockSize[1] * blockSize[2]];
		floatBlock = new float[blockSize[0] * blockSize[1] * blockSize[2]];
		doubleBlock = new double[blockSize[0] * blockSize[1] * blockSize[2]];
		rnd.nextBytes(byteBlock);
		for(int i = 0; i < floatBlock.length; ++i) {
			shortBlock[i] = (short)rnd.nextInt();
			intBlock[i] = rnd.nextInt();
			longBlock[i] = rnd.nextLong();
			floatBlock[i] = Float.intBitsToFloat(rnd.nextInt());
			doubleBlock[i] = Double.longBitsToDouble(rnd.nextLong());
		}
	}

	/**
	 * @throws IOException
	 */
	@AfterClass
	public static void rampDownAfterClass() throws IOException {

		Assert.assertTrue(n5.remove());
	}

	@Test
	public void testCreateGroup() {

		try {
			n5.createGroup(groupName);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		final Path groupPath = Paths.get(groupName);
		for (int i = 0; i < groupPath.getNameCount(); ++i)
			if (!n5.exists(groupPath.subpath(0, i + 1).toString()))
				fail("Group does not exist");
	}

	@Test
	public void testCreateDataset() {

		try {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, CompressionType.RAW);
		} catch (final IOException e) {
			fail(e.getMessage());
		}

		if (!n5.exists(datasetName))
			fail("Dataset does not exist");

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
	public void testWriteReadByteBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT8,
					DataType.INT8}) {

				System.out.println("Testing " + compressionType + " " + dataType);
				try {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compressionType);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(blockSize, new long[]{0, 0, 0}, byteBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

					Assert.assertArrayEquals(byteBlock, (byte[])loadedDataBlock.getData());

					Assert.assertTrue(n5.remove(datasetName));

				} catch (final IOException e) {
					e.printStackTrace();
					fail("Block cannot be written.");
				}
			}
		}
	}

	@Test
	public void testWriteReadShortBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT16,
					DataType.INT16}) {

				System.out.println("Testing " + compressionType + " " + dataType);
				try {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compressionType);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ShortArrayDataBlock dataBlock = new ShortArrayDataBlock(blockSize, new long[]{0, 0, 0}, shortBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

					Assert.assertArrayEquals(shortBlock, (short[])loadedDataBlock.getData());

					Assert.assertTrue(n5.remove(datasetName));

				} catch (final IOException e) {
					e.printStackTrace();
					fail("Block cannot be written.");
				}
			}
		}
	}

	@Test
	public void testWriteReadIntBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT32,
					DataType.INT32}) {

				System.out.println("Testing " + compressionType + " " + dataType);
				try {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compressionType);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final IntArrayDataBlock dataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, intBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

					Assert.assertArrayEquals(intBlock, (int[])loadedDataBlock.getData());

					Assert.assertTrue(n5.remove(datasetName));

				} catch (final IOException e) {
					e.printStackTrace();
					fail("Block cannot be written.");
				}
			}
		}
	}

	@Test
	public void testWriteReadLongBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT64,
					DataType.INT64}) {

				System.out.println("Testing " + compressionType + " " + dataType);
				try {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compressionType);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final LongArrayDataBlock dataBlock = new LongArrayDataBlock(blockSize, new long[]{0, 0, 0}, longBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

					Assert.assertArrayEquals(longBlock, (long[])loadedDataBlock.getData());

					Assert.assertTrue(n5.remove(datasetName));

				} catch (final IOException e) {
					e.printStackTrace();
					fail("Block cannot be written.");
				}
			}
		}
	}

	@Test
	public void testWriteReadFloatBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			System.out.println("Testing " + compressionType + " float32");
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT32, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final FloatArrayDataBlock dataBlock = new FloatArrayDataBlock(blockSize, new long[]{0, 0, 0}, floatBlock);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

				Assert.assertArrayEquals(floatBlock, (float[])loadedDataBlock.getData(), 0.001f);

				Assert.assertTrue(n5.remove(datasetName));

			} catch (final IOException e) {
				e.printStackTrace();
				fail("Block cannot be written.");
			}
		}
	}


	@Test
	public void testWriteReadDoubleBlock() {

		for (final CompressionType compressionType : CompressionType.values()) {
			System.out.println("Testing " + compressionType + " float64");
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final DoubleArrayDataBlock dataBlock = new DoubleArrayDataBlock(blockSize, new long[]{0, 0, 0}, doubleBlock);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

				Assert.assertArrayEquals(doubleBlock, (double[])loadedDataBlock.getData(), 0.001);

				Assert.assertTrue(n5.remove(datasetName));

			} catch (final IOException e) {
				e.printStackTrace();
				fail("Block cannot be written.");
			}
		}
	}

	@Test
	public void testMode1WriteReadByteBlock() {

		final int[] differentBlockSize = new int[] {5, 10, 15};

		for (final CompressionType compressionType : CompressionType.values()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT8,
					DataType.INT8}) {

				System.out.println("Testing " + compressionType + " " + dataType + " (mode=1)");
				try {
					n5.createDataset(datasetName, dimensions, differentBlockSize, dataType, compressionType);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(differentBlockSize, new long[]{0, 0, 0}, byteBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});

					Assert.assertArrayEquals(byteBlock, (byte[])loadedDataBlock.getData());

					Assert.assertTrue(n5.remove(datasetName));

				} catch (final IOException e) {
					e.printStackTrace();
					fail("Block cannot be written.");
				}
			}
		}
	}

	@Test
	public void testOverwriteBlock() {

		try {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.INT32, CompressionType.GZIP);
			final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);

			final IntArrayDataBlock randomDataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, intBlock);
			n5.writeBlock(datasetName, attributes, randomDataBlock);
			final DataBlock<?> loadedRandomDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});
			Assert.assertArrayEquals(intBlock, (int[])loadedRandomDataBlock.getData());

			// test the case where the resulting file becomes shorter
			final IntArrayDataBlock emptyDataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, new int[DataBlock.getNumElements(blockSize)]);
			n5.writeBlock(datasetName, attributes, emptyDataBlock);
			final DataBlock<?> loadedEmptyDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});
			Assert.assertArrayEquals(new int[DataBlock.getNumElements(blockSize)], (int[])loadedEmptyDataBlock.getData());

			Assert.assertTrue(n5.remove(datasetName));

		} catch (final IOException e) {
			e.printStackTrace();
			fail("Block cannot be written.");
		}
	}

	@Test
	public void testAttributes() {

		try {
			n5.createGroup(groupName);

			n5.setAttribute(groupName, "key1", "value1");
			Assert.assertEquals(1, n5Parser.getAttributes(groupName).size());
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));

			final Map<String, String> newAttributes = new HashMap<>();
			newAttributes.put("key2", "value2");
			newAttributes.put("key3", "value3");
			n5.setAttributes(groupName, newAttributes);
			Assert.assertEquals(3, n5Parser.getAttributes(groupName).size());
			Assert.assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			Assert.assertEquals("value2", n5.getAttribute(groupName, "key2", String.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));

			// test the case where the resulting file becomes shorter
			n5.setAttribute(groupName, "key1", new Integer(1));
			n5.setAttribute(groupName, "key2", new Integer(2));
			Assert.assertEquals(3, n5Parser.getAttributes(groupName).size());
			Assert.assertEquals(new Integer(1), n5.getAttribute(groupName, "key1", Integer.class));
			Assert.assertEquals(new Integer(2), n5.getAttribute(groupName, "key2", Integer.class));
			Assert.assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));

		} catch (final IOException e) {
			fail(e.getMessage());
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

		if (n5.exists(groupName))
			fail("Group still exists");
	}

	@Test
	public void testList() {

		try {
			n5.createGroup(groupName);
			for (final String subGroup : subGroupNames)
				n5.createGroup(groupName + "/" + subGroup);

			final String[] groupsList = n5.list(groupName);
			Arrays.sort(groupsList);

			Assert.assertArrayEquals(subGroupNames, groupsList);

			// test listing the root group ("" and "/" should give identical results)
			Assert.assertArrayEquals(new String[] {"test"}, n5.list(""));
			Assert.assertArrayEquals(new String[] {"test"}, n5.list("/"));
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testExists() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, CompressionType.RAW);
			Assert.assertTrue(n5.exists(datasetName2));
			Assert.assertTrue(n5.datasetExists(datasetName2));

			n5.createGroup(groupName2);
			Assert.assertTrue(n5.exists(groupName2));
			Assert.assertFalse(n5.datasetExists(groupName2));
			Assert.assertTrue(n5Parser.getAttributes(groupName2).isEmpty());
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testListAttributes() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		try {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, CompressionType.RAW);
			n5.setAttribute(datasetName2, "attr1", new double[] {1, 2, 3});
			n5.setAttribute(datasetName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(datasetName2, "attr3", 1.0);
			n5.setAttribute(datasetName2, "attr4", "a");

			Map<String, Class<?>> attributesMap = n5.listAttributes(datasetName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);

			n5.createGroup(groupName2);
			n5.setAttribute(groupName2, "attr1", new double[] {1, 2, 3});
			n5.setAttribute(groupName2, "attr2", new String[] {"a", "b", "c"});
			n5.setAttribute(groupName2, "attr3", 1.0);
			n5.setAttribute(groupName2, "attr4", "a");

			attributesMap = n5.listAttributes(datasetName2);
			Assert.assertTrue(attributesMap.get("attr1") == double[].class);
			Assert.assertTrue(attributesMap.get("attr2") == String[].class);
			Assert.assertTrue(attributesMap.get("attr3") == double.class);
			Assert.assertTrue(attributesMap.get("attr4") == String.class);
		} catch (final IOException e) {
			fail(e.getMessage());
		}
	}
}
