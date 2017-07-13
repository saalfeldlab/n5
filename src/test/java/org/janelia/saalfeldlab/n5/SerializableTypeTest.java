package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SerializableTypeTest {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";

	static private String datasetName = "/test/group/dataset";

	static private long[] dimensions = new long[]{100, 200, 300};

	static private int[] blockSize = new int[]{33, 22, 11};
	
	static private Random rnd = new Random();
	
	@Before
	public void before() {
		cleanup();
	}
	
	@After
	public void after() {
		cleanup();
	}
	
	private void cleanup() {
		try {
			N5.openFSWriter(testDirPath).remove("");
		} catch (final IOException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testString() {
		
		final String[] data = new String[DataBlock.getNumElements(blockSize)];
		for (int i = 0; i < data.length; ++i) {
			final int len = rnd.nextInt(50);
			data[i] = RandomStringUtils.randomAlphanumeric(len);
		}
		
		final N5Writer n5 = N5.openFSWriter(testDirPath);
		for (final CompressionType compressionType : CompressionType.values()) {
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.SERIALIZABLE, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				
				final SerializableArrayDataBlock<String> dataBlock = new SerializableArrayDataBlock<>(blockSize, new long[]{0, 0, 0}, data);
				n5.writeBlock(datasetName, attributes, dataBlock);
				
				final DataBlock<?> readDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});
	
				final String[] readData = (String[])readDataBlock.getData();
				Assert.assertArrayEquals(data, readData);
	
				Assert.assertTrue(n5.remove(datasetName));
				
			} catch (final IOException e) {
				Assert.fail(e.getMessage());
			}
		}
	}
	
	@Test
	public void testHashSet() {
		
		@SuppressWarnings("unchecked")
		final HashSet<Integer>[] data = new HashSet[DataBlock.getNumElements(blockSize)];
		for (int i = 0; i < data.length; ++i) {
			data[i] = new HashSet<>();
			final int cnt = rnd.nextInt(10);
			for (int j = 0; j < cnt; ++j)
				data[i].add(rnd.nextInt(100));
		}
		
		final N5Writer n5 = N5.openFSWriter(testDirPath);
		for (final CompressionType compressionType : CompressionType.values()) {
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.SERIALIZABLE, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				
				final SerializableArrayDataBlock<HashSet<Integer>> dataBlock = new SerializableArrayDataBlock<>(blockSize, new long[]{0, 0, 0}, data);
				n5.writeBlock(datasetName, attributes, dataBlock);
				
				final DataBlock<?> readDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});
	
				@SuppressWarnings("unchecked")
				final HashSet<Integer>[] readData = (HashSet[])readDataBlock.getData();
				Assert.assertEquals(data.length, readData.length);
				for (int i = 0; i < data.length; ++i)
					Assert.assertArrayEquals(new TreeSet<>(data[i]).toArray(), new TreeSet<>(readData[i]).toArray());
	
				Assert.assertTrue(n5.remove(datasetName));
				
			} catch (final IOException e) {
				Assert.fail(e.getMessage());
			}
		}
	}
	
	@Test
	public void testBigInteger() {
		
		final BigInteger[] data = new BigInteger[DataBlock.getNumElements(blockSize)];
		for (int i = 0; i < data.length; ++i) {
			final int bits = rnd.nextInt(256) + 128;
			data[i] = new BigInteger(bits, rnd);
		}
		
		final N5Writer n5 = N5.openFSWriter(testDirPath);
		for (final CompressionType compressionType : CompressionType.values()) {
			try {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.SERIALIZABLE, compressionType);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				
				final SerializableArrayDataBlock<BigInteger> dataBlock = new SerializableArrayDataBlock<>(blockSize, new long[]{0, 0, 0}, data);
				n5.writeBlock(datasetName, attributes, dataBlock);
				
				final DataBlock<?> readDataBlock = n5.readBlock(datasetName, attributes, new long[]{0, 0, 0});
	
				final BigInteger[] readData = (BigInteger[])readDataBlock.getData();
				Assert.assertArrayEquals(data, readData);
	
				Assert.assertTrue(n5.remove(datasetName));
				
			} catch (final IOException e) {
				Assert.fail(e.getMessage());
			}
		}
	}
}
