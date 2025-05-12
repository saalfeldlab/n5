/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.janelia.saalfeldlab.n5.N5Exception.N5ClassCastException;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.codec.AsTypeCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Abstract base class for testing N5 functionality.
 * Subclasses are expected to provide a specific N5 implementation to be tested by defining the {@link #createN5Writer()} method.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 * @author John Bogovic &lt;bogovicj@janelia.hhmi.org&gt;
 * @author Caleb Hulbert &lt;hulbertc@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5Test {

	static protected final String groupName = "/test/group";
	static protected final String[] subGroupNames = new String[]{"a", "b", "c"};
	static protected final String datasetName = "/test/group/dataset";
	static protected final long[] dimensions = new long[]{100, 200, 300};
	static protected final int[] blockSize = new int[]{44, 33, 22};
	static protected final int blockNumElements = blockSize[0] * blockSize[1] * blockSize[2];

	static protected byte[] byteBlock;
	static protected short[] shortBlock;
	static protected int[] intBlock;
	static protected long[] longBlock;
	static protected float[] floatBlock;
	static protected double[] doubleBlock;

	protected final HashSet<N5Writer> tempWriters = new HashSet<>();

	public N5Writer createTempN5Writer() {

		try {
			return createTempN5Writer(tempN5Location());
		} catch (URISyntaxException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final N5Writer createTempN5Writer(String location) {

		return createTempN5Writer(location, new GsonBuilder());
	}

	protected final N5Writer createTempN5Writer(String location, GsonBuilder gson) {

		final N5Writer tempWriter;
		try {
			tempWriter = createN5Writer(location, gson);
		} catch (IOException | URISyntaxException e) {
			throw new RuntimeException(e);
		}
		tempWriters.add(tempWriter);
		return tempWriter;
	}

	@After
	public void removeTempWriters() {

		synchronized (tempWriters) {
			for (final N5Writer writer : tempWriters) {
				try {
					writer.remove();
				} catch (final Exception e) {
				}
			}
			tempWriters.clear();
		}
	}

	protected abstract String tempN5Location() throws URISyntaxException, IOException;

	protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return createN5Writer(tempN5Location());
	}

	protected N5Writer createN5Writer(final String location) throws IOException, URISyntaxException {

		return createN5Writer(location, new GsonBuilder());
	}

	/* Tests that overide this should enusre that the `N5Writer` created will remove its container on close() */
	protected abstract N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException, URISyntaxException;

	protected N5Reader createN5Reader(final String location) throws IOException, URISyntaxException {

		return createN5Reader(location, new GsonBuilder());
	}

	protected abstract N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException, URISyntaxException;

	protected Compression[] getCompressions() {

		return new Compression[]{
				new RawCompression(),
//				new Bzip2Compression(),
//				new GzipCompression(),
//				new GzipCompression(5, true),
//				new Lz4Compression(),
//				new XzCompression()
		};
	}

	@Before
	public void setUpOnce() {

		final Random rnd = new Random();
		byteBlock = new byte[blockNumElements];
		shortBlock = new short[blockNumElements];
		intBlock = new int[blockNumElements];
		longBlock = new long[blockNumElements];
		floatBlock = new float[blockNumElements];
		doubleBlock = new double[blockNumElements];
		rnd.nextBytes(byteBlock);
		for (int i = 0; i < blockNumElements; ++i) {
			shortBlock[i] = (short)rnd.nextInt();
			intBlock[i] = rnd.nextInt();
			longBlock[i] = rnd.nextLong();
			floatBlock[i] = Float.intBitsToFloat(rnd.nextInt());
			doubleBlock[i] = Double.longBitsToDouble(rnd.nextLong());
		}
	}

	@Test
	public void testCreateGroup() {

		try (N5Writer n5 = createTempN5Writer()) {
			n5.createGroup(groupName);
			final Path groupPath = Paths.get(groupName);
			String subGroup = "";
			for (int i = 0; i < groupPath.getNameCount(); ++i) {
				subGroup = subGroup + "/" + groupPath.getName(i);
				assertTrue("Group does not exist: " + subGroup, n5.exists(subGroup));
			}
		}
	}

	@Test
	public void testSetAttributeDoesntCreateGroup() {

		try (final N5Writer writer = createTempN5Writer()) {
			final String testGroup = "/group/should/not/exit";
			assertFalse(writer.exists(testGroup));
			assertThrows(N5Exception.N5IOException.class, () -> writer.setAttribute(testGroup, "test", "test"));
			assertFalse(writer.exists(testGroup));
		}
	}

	@Test
	public void testCreateDataset() {

		final DatasetAttributes info;
		try (N5Writer writer = createTempN5Writer()) {
			writer.createDataset(datasetName, dimensions, blockSize, DataType.UINT64);

			assertTrue("Dataset does not exist", writer.exists(datasetName));

			info = writer.getDatasetAttributes(datasetName);
		}
		assertArrayEquals(dimensions, info.getDimensions());
		assertArrayEquals(blockSize, info.getBlockSize());
		assertEquals(DataType.UINT64, info.getDataType());
		assertTrue(info.getCompression() instanceof RawCompression);
	}

	@Test
	public void testWriteReadByteBlock() {

		for (final Compression compression : getCompressions()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT8, DataType.INT8}) {

				try (final N5Writer n5 = createTempN5Writer()) {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compression);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(blockSize, new long[]{0, 0, 0}, byteBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);
					assertArrayEquals(byteBlock, (byte[])loadedDataBlock.getData());
					assertTrue(n5.remove(datasetName));

				}
			}
		}
	}

	@Test
	public void testWriteReadByteBlockMultipleCodecs() {

		/*TODO: this tests "passes" in the sense that we get the correct output, but it
		*  maybe is not the behavior we actually want*/

		try (final N5Writer n5 = createTempN5Writer()) {
			final String dataset = "8_64_32";
			n5.remove(dataset);
			final Codec[] codecs = {
					new N5BlockCodec<>(),
					new AsTypeCodec(DataType.INT8, DataType.INT32),
					new AsTypeCodec(DataType.INT32, DataType.INT64)
			};
			final byte[] byteBlock1 = new byte[]{1,2,3,4,5,6,7,8};
			final long[] dimensions1 = new long[]{2,2,2};
			final int[] blockSize1 = new int[]{2,2,2};
			final DatasetAttributes attrs = new DatasetAttributes(dimensions1, blockSize1, DataType.INT8, codecs);
			n5.createDataset(dataset, attrs);
			final DatasetAttributes attributes = n5.getDatasetAttributes(dataset);
			final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(blockSize1, new long[]{0, 0, 0}, byteBlock1);
			n5.writeBlock(dataset, attributes, dataBlock);

			final DataBlock<?> loadedDataBlock = n5.readBlock(dataset, attrs, 0, 0, 0);
			assertArrayEquals(byteBlock1, (byte[])loadedDataBlock.getData());
		}
	}

	@Test
	public void testWriteReadStringBlock() throws IOException, URISyntaxException {

		// test dataset; all characters are valid UTF8 but may have different numbers of bytes!
		final DataType dataType = DataType.STRING;
		final int[] blockSize = new int[]{3, 2, 1};
		final String[] stringBlock = new String[]{"", "a", "bc", "de", "fgh", ":-þ"};

		for (final Compression compression : getCompressions()) {
			try (final N5Writer n5 = createN5Writer("test.n5")) {
				n5.createDataset(datasetName, dimensions, blockSize, dataType, compression);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final StringDataBlock dataBlock = new StringDataBlock(blockSize, new long[]{0L, 0L, 0L}, stringBlock);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0L, 0L, 0L);

				assertArrayEquals(stringBlock, (String[])loadedDataBlock.getData());

//				assertTrue(n5.remove(datasetName));

			}
		}
	}

	@Test
	public void testWriteReadShortBlock() {

		for (final Compression compression : getCompressions()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT16,
					DataType.INT16}) {

				try (final N5Writer n5 = createTempN5Writer()) {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compression);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ShortArrayDataBlock dataBlock = new ShortArrayDataBlock(blockSize, new long[]{0, 0, 0}, shortBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

					assertArrayEquals(shortBlock, (short[])loadedDataBlock.getData());

					assertTrue(n5.remove(datasetName));

				}
			}
		}
	}

	@Test
	public void testWriteReadIntBlock() {

		for (final Compression compression : getCompressions()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT32,
					DataType.INT32}) {

				try (final N5Writer n5 = createTempN5Writer()) {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, (Codec)compression);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final IntArrayDataBlock dataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, intBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

					assertArrayEquals(intBlock, (int[])loadedDataBlock.getData());

					assertTrue(n5.remove(datasetName));

				}
			}
		}
	}

	@Test
	public void testWriteReadLongBlock() {

		for (final Compression compression : getCompressions()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT64,
					DataType.INT64}) {

				try (final N5Writer n5 = createTempN5Writer()) {
					n5.createDataset(datasetName, dimensions, blockSize, dataType, compression);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final LongArrayDataBlock dataBlock = new LongArrayDataBlock(blockSize, new long[]{0, 0, 0}, longBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

					assertArrayEquals(longBlock, (long[])loadedDataBlock.getData());

					assertTrue(n5.remove(datasetName));

				}
			}
		}
	}

	@Test
	public void testWriteReadFloatBlock() {

		for (final Compression compression : getCompressions()) {
			try (final N5Writer n5 = createTempN5Writer()) {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT32, compression);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final FloatArrayDataBlock dataBlock = new FloatArrayDataBlock(blockSize, new long[]{0, 0, 0}, floatBlock);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

				assertArrayEquals(floatBlock, (float[])loadedDataBlock.getData(), 0.001f);

				assertTrue(n5.remove(datasetName));

			}
		}
	}

	@Test
	public void testWriteReadDoubleBlock() {

		for (final Compression compression : getCompressions()) {
			try (final N5Writer n5 = createTempN5Writer()) {
				n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, compression);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
				final DoubleArrayDataBlock dataBlock = new DoubleArrayDataBlock(blockSize, new long[]{0, 0, 0}, doubleBlock);
				n5.writeBlock(datasetName, attributes, dataBlock);

				final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

				assertArrayEquals(doubleBlock, (double[])loadedDataBlock.getData(), 0.001);

				assertTrue(n5.remove(datasetName));

			}
		}
	}

	@Test
	public void testMode1WriteReadByteBlock() {

		final int[] differentBlockSize = new int[]{5, 10, 15};

		for (final Compression compression : getCompressions()) {
			for (final DataType dataType : new DataType[]{
					DataType.UINT8,
					DataType.INT8}) {

				try (final N5Writer n5 = createTempN5Writer()) {
					n5.createDataset(datasetName, dimensions, differentBlockSize, dataType, compression);
					final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
					final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(differentBlockSize, new long[]{0, 0, 0}, byteBlock);
					n5.writeBlock(datasetName, attributes, dataBlock);

					final DataBlock<?> loadedDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);

					assertArrayEquals(byteBlock, (byte[])loadedDataBlock.getData());

					assertTrue(n5.remove(datasetName));

				}
			}
		}
	}

	@Test
	public void testWriteReadSerializableBlock() throws ClassNotFoundException {

		for (final Compression compression : getCompressions()) {

			final DataType dataType = DataType.OBJECT;
			try (final N5Writer n5 = createTempN5Writer()) {
				n5.createDataset(datasetName, dimensions, blockSize, dataType, compression);
				final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);

				final HashMap<String, ArrayList<double[]>> object = new HashMap<>();
				object.put("one", new ArrayList<>());
				object.put("two", new ArrayList<>());
				object.get("one").add(new double[]{1, 2, 3});
				object.get("two").add(new double[]{4, 5, 6, 7, 8});

				n5.writeSerializedBlock(object, datasetName, attributes, 0, 0, 0);

				final HashMap<String, ArrayList<double[]>> loadedObject = n5.readSerializedBlock(datasetName, attributes, new long[]{0, 0, 0});

				object.forEach((key, value) -> assertArrayEquals(value.get(0), loadedObject.get(key).get(0), 0.01));

				assertTrue(n5.remove(datasetName));

			}
		}
	}

	@Test
	public void testOverwriteBlock() {

		try (final N5Writer n5 = createTempN5Writer("test.n5")) {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.INT32, new GzipCompression());
			final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);

			final IntArrayDataBlock randomDataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, intBlock);
			n5.writeBlock(datasetName, attributes, randomDataBlock);
			final DataBlock<?> loadedRandomDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);
			assertArrayEquals(intBlock, (int[])loadedRandomDataBlock.getData());

			// test the case where the resulting file becomes shorter (because the data compresses better)
			final int[] emptyBlock = new int[DataBlock.getNumElements(blockSize)];
			final IntArrayDataBlock emptyDataBlock = new IntArrayDataBlock(blockSize, new long[]{0, 0, 0}, emptyBlock);
			n5.writeBlock(datasetName, attributes, emptyDataBlock);
			final DataBlock<?> loadedEmptyDataBlock = n5.readBlock(datasetName, attributes, 0, 0, 0);
			assertArrayEquals(emptyBlock, (int[])loadedEmptyDataBlock.getData());

			assertTrue(n5.remove(datasetName));

		}
	}

	@Test
	public void testAttributeParsingPrimitive() {

		try (final N5Writer n5 = createTempN5Writer()) {

			n5.createGroup(groupName);

			/* Test parsing of int, int[], double, double[], String, and String[] types
			 *
			 *	All types are parseable as JsonElements or String
			 *
			 *	ints are also parseable as doubles and Strings
			 *	doubles are also parseable as ints and Strings
			 *	Strings should be parsable as Strings
			 *
			 *	int[]s should be parseable as double[]s and String[]s
			 *	double[]s should be parseable as double[]s and String[]s
			 *	String[]s should be parsable as String[]s
			 */

			n5.setAttribute(groupName, "key", "value");
			assertEquals("value", n5.getAttribute(groupName, "key", String.class));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Integer.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", int[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Double.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", double[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", String[].class));

			n5.setAttribute(groupName, "key", new String[]{"value"});
			assertArrayEquals(new String[]{"value"}, n5.getAttribute(groupName, "key", String[].class));
			assertEquals(JsonParser.parseString("[\"value\"]"), JsonParser.parseString(n5.getAttribute(groupName, "key", String.class)));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Integer.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", int[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Double.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", double[].class));

			n5.setAttribute(groupName, "key", 1);
			assertEquals(1, (long)n5.getAttribute(groupName, "key", Integer.class));
			assertEquals(1.0, n5.getAttribute(groupName, "key", Double.class), 1e-9);
			assertEquals("1", n5.getAttribute(groupName, "key", String.class));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", int[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", double[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", String[].class));

			n5.setAttribute(groupName, "key", new int[]{2, 3});
			assertArrayEquals(new int[]{2, 3}, n5.getAttribute(groupName, "key", int[].class));
			assertArrayEquals(new double[]{2.0, 3.0}, n5.getAttribute(groupName, "key", double[].class), 1e-9);
			assertEquals(JsonParser.parseString("[2,3]"), JsonParser.parseString(n5.getAttribute(groupName, "key", String.class)));
			assertArrayEquals(new String[]{"2", "3"}, n5.getAttribute(groupName, "key", String[].class));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Integer.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Double.class));

			n5.setAttribute(groupName, "key", 0.1);
			assertEquals(0, (long)n5.getAttribute(groupName, "key", Integer.class));
			assertEquals(0.1, n5.getAttribute(groupName, "key", Double.class), 1e-9);
			assertEquals("0.1", n5.getAttribute(groupName, "key", String.class));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", int[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", double[].class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", String[].class));

			n5.setAttribute(groupName, "key", new double[]{0.2, 0.3});
			assertArrayEquals(new int[]{0, 0}, n5.getAttribute(groupName, "key", int[].class)); // TODO returns not null, is this right?
			assertArrayEquals(new double[]{0.2, 0.3}, n5.getAttribute(groupName, "key", double[].class), 1e-9);
			assertEquals(JsonParser.parseString("[0.2,0.3]"), JsonParser.parseString(n5.getAttribute(groupName, "key", String.class)));
			assertArrayEquals(new String[]{"0.2", "0.3"}, n5.getAttribute(groupName, "key", String[].class));
			assertNotNull(n5.getAttribute(groupName, "key", JsonElement.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Integer.class));
			assertThrows(N5ClassCastException.class, () -> n5.getAttribute(groupName, "key", Double.class));
		}
	}

	@Test
	public void testAttributes() {

		try (final N5Writer n5 = createTempN5Writer()) {
			assertNull(n5.getAttribute(groupName, "test", String.class));
			assertEquals(0, n5.listAttributes(groupName).size());
			n5.createGroup(groupName);
			assertNull(n5.getAttribute(groupName, "test", String.class));

			assertEquals(0, n5.listAttributes(groupName).size());

			n5.setAttribute(groupName, "key1", "value1");
			assertEquals(1, n5.listAttributes(groupName).size());

			/* class interface */
			assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			/* type interface */
			assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>() {

			}.getType()));

			final Map<String, String> newAttributes = new HashMap<>();
			newAttributes.put("key2", "value2");
			newAttributes.put("key3", "value3");
			n5.setAttributes(groupName, newAttributes);
			assertEquals(3, n5.listAttributes(groupName).size());
			/* class interface */
			assertEquals("value1", n5.getAttribute(groupName, "key1", String.class));
			assertEquals("value2", n5.getAttribute(groupName, "key2", String.class));
			assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			assertEquals("value1", n5.getAttribute(groupName, "key1", new TypeToken<String>() {

			}.getType()));
			assertEquals("value2", n5.getAttribute(groupName, "key2", new TypeToken<String>() {

			}.getType()));
			assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>() {

			}.getType()));

			// test the case where the resulting file becomes shorter
			n5.setAttribute(groupName, "key1", 1);
			n5.setAttribute(groupName, "key2", 2);
			assertEquals(3, n5.listAttributes(groupName).size());
			/* class interface */
			assertEquals(Integer.valueOf(1), n5.getAttribute(groupName, "key1", Integer.class));
			assertEquals(Integer.valueOf(2), n5.getAttribute(groupName, "key2", Integer.class));
			assertEquals("value3", n5.getAttribute(groupName, "key3", String.class));
			/* type interface */
			assertEquals(Integer.valueOf(1), n5.getAttribute(groupName, "key1", new TypeToken<Integer>() {

			}.getType()));
			assertEquals(Integer.valueOf(2), n5.getAttribute(groupName, "key2", new TypeToken<Integer>() {

			}.getType()));
			assertEquals("value3", n5.getAttribute(groupName, "key3", new TypeToken<String>() {

			}.getType()));

			n5.setAttribute(groupName, "key1", null);
			n5.setAttribute(groupName, "key2", null);
			n5.setAttribute(groupName, "key3", null);
			assertEquals(0, n5.listAttributes(groupName).size());
		}
	}

	@Test
	public void testNullAttributes() throws URISyntaxException, IOException {

		/* serializeNulls*/
		try (N5Writer writer = createTempN5Writer(tempN5Location(), new GsonBuilder().serializeNulls())) {

			writer.createGroup(groupName);
			writer.setAttribute(groupName, "nullValue", null);
			assertNull(writer.getAttribute(groupName, "nullValue", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "nullValue", JsonElement.class));
			final HashMap<String, Object> nulls = new HashMap<>();
			nulls.put("anotherNullValue", null);
			nulls.put("structured/nullValue", null);
			nulls.put("implicitNulls[3]", null);
			writer.setAttributes(groupName, nulls);

			assertNull(writer.getAttribute(groupName, "anotherNullValue", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "anotherNullValue", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "structured/nullValue", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "structured/nullValue", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "implicitNulls[3]", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "implicitNulls[3]", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "implicitNulls[1]", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "implicitNulls[1]", JsonElement.class));

			/* Negative test; a value that truly doesn't exist will still return `null` but will also return `null` when querying as a `JsonElement` */
			assertNull(writer.getAttribute(groupName, "implicitNulls[10]", Object.class));
			assertNull(writer.getAttribute(groupName, "implicitNulls[10]", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "keyDoesn'tExist", Object.class));
			assertNull(writer.getAttribute(groupName, "keyDoesn'tExist", JsonElement.class));

			/* check existing value gets overwritten */
			writer.setAttribute(groupName, "existingValue", 1);
			assertEquals((Integer)1, writer.getAttribute(groupName, "existingValue", Integer.class));
			writer.setAttribute(groupName, "existingValue", null);
			assertThrows(N5ClassCastException.class, () -> writer.getAttribute(groupName, "existingValue", Integer.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "existingValue", JsonElement.class));
		}

		/* without serializeNulls*/
		try (N5Writer writer = createTempN5Writer(tempN5Location(), new GsonBuilder())) {

			writer.createGroup(groupName);
			writer.setAttribute(groupName, "nullValue", null);
			assertNull(writer.getAttribute(groupName, "nullValue", Object.class));
			assertNull(writer.getAttribute(groupName, "nullValue", JsonElement.class));
			final HashMap<String, Object> nulls = new HashMap<>();
			nulls.put("anotherNullValue", null);
			nulls.put("structured/nullValue", null);
			nulls.put("implicitNulls[3]", null);
			writer.setAttributes(groupName, nulls);

			assertNull(writer.getAttribute(groupName, "anotherNullValue", Object.class));
			assertNull(writer.getAttribute(groupName, "anotherNullValue", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "structured/nullValue", Object.class));
			assertNull(writer.getAttribute(groupName, "structured/nullValue", JsonElement.class));

			/* Arrays are still filled with `null`, regardless of `serializeNulls()`*/
			assertNull(writer.getAttribute(groupName, "implicitNulls[3]", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "implicitNulls[3]", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "implicitNulls[1]", Object.class));
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(groupName, "implicitNulls[1]", JsonElement.class));

			/* Negative test; a value that truly doesn't exist will still return `null` but will also return `null` when querying as a `JsonElement` */
			assertNull(writer.getAttribute(groupName, "implicitNulls[10]", Object.class));
			assertNull(writer.getAttribute(groupName, "implicitNulls[10]", JsonElement.class));

			assertNull(writer.getAttribute(groupName, "keyDoesn'tExist", Object.class));
			assertNull(writer.getAttribute(groupName, "keyDoesn'tExist", JsonElement.class));

			/* check existing value gets overwritten */
			writer.setAttribute(groupName, "existingValue", 1);
			assertEquals((Integer)1, writer.getAttribute(groupName, "existingValue", Integer.class));
			writer.setAttribute(groupName, "existingValue", null);
			assertNull(writer.getAttribute(groupName, "existingValue", Integer.class));
			assertNull(writer.getAttribute(groupName, "existingValue", JsonElement.class));
		}
	}

	@Test
	public void testRemoveAttributes() throws IOException, URISyntaxException {

		try (N5Writer writer = createTempN5Writer(tempN5Location(), new GsonBuilder().serializeNulls())) {

			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			/* Remove Test without Type */
			assertTrue(writer.removeAttribute("", "a/b/c"));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));

			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			/* Remove Test with correct Type */
			assertEquals((Integer)100, writer.removeAttribute("", "a/b/c", Integer.class));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));

			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			/* Remove Test with incorrect Type */
			assertThrows(N5ClassCastException.class, () -> writer.removeAttribute("", "a/b/c", Boolean.class));
			final Integer abcInteger = writer.removeAttribute("", "a/b/c", Integer.class);
			assertEquals((Integer)100, abcInteger);
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));

			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			/* Remove Test with non-leaf */
			assertTrue(writer.removeAttribute("", "a/b"));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));
			assertNull(writer.getAttribute("", "a/b", JsonObject.class));

			writer.setAttribute("", "a\\b\\c/b\\[10]\\c/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a\\b\\c/b\\[10]\\c/c", Integer.class));
			/* Remove Test with escape-requiring key  */
			assertTrue(writer.removeAttribute("", "a\\b\\c/b\\[10]\\c/c"));
			assertNull(writer.getAttribute("", "a\\b\\c/b\\[10]\\c/c", Integer.class));

			writer.setAttribute("", "a/b[9]", 10);
			assertEquals((Integer)10, writer.getAttribute("", "a/b[9]", Integer.class));
			assertEquals((Integer)0, writer.getAttribute("", "a/b[8]", Integer.class));
			/*Remove test with arrays */
			assertTrue(writer.removeAttribute("", "a/b[5]"));
			assertEquals(9, writer.getAttribute("", "a/b", JsonArray.class).size());
			assertEquals((Integer)0, writer.getAttribute("", "a/b[5]", Integer.class));
			assertEquals((Integer)10, writer.getAttribute("", "a/b[8]", Integer.class));
			assertTrue(writer.removeAttribute("", "a/b[8]"));
			assertEquals(8, writer.getAttribute("", "a/b", JsonArray.class).size());
			assertNull(writer.getAttribute("", "a/b[8]", Integer.class));
			assertTrue(writer.removeAttribute("", "a/b"));
			assertNull(writer.getAttribute("", "a/b[9]", Integer.class));
			assertNull(writer.getAttribute("", "a/b", Integer.class));

			/* ensure old remove behavior no longer works (i.e. set to null no longer should remove) */
			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			writer.setAttribute("", "a/b/c", null);
			assertEquals(JsonNull.INSTANCE, writer.getAttribute("", "a/b/c", JsonNull.class));
			writer.removeAttribute("", "a/b/c");
			assertNull(writer.getAttribute("", "a/b/c", JsonNull.class));

			/* remove multiple, all present */
			writer.setAttribute("", "a/b/c", 100);
			writer.setAttribute("", "a/b/d", "test");
			writer.setAttribute("", "a/c[9]", 10);
			writer.setAttribute("", "a/c[5]", 5);

			assertTrue(writer.removeAttributes("", Arrays.asList("a/b/c", "a/b/d", "a/c[5]")));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));
			assertNull(writer.getAttribute("", "a/b/d", String.class));
			assertEquals(9, writer.getAttribute("", "a/c", JsonArray.class).size());
			assertEquals((Integer)10, writer.getAttribute("", "a/c[8]", Integer.class));
			assertEquals((Integer)0, writer.getAttribute("", "a/c[5]", Integer.class));

			/* remove multiple, any  present */
			writer.setAttribute("", "a/b/c", 100);
			writer.setAttribute("", "a/b/d", "test");
			writer.setAttribute("", "a/c[9]", 10);
			writer.setAttribute("", "a/c[5]", 5);

			assertTrue(writer.removeAttributes("", Arrays.asList("a/b/c", "a/b/d", "a/x[5]")));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));
			assertNull(writer.getAttribute("", "a/b/d", String.class));
			assertEquals(10, writer.getAttribute("", "a/c", JsonArray.class).size());
			assertEquals((Integer)10, writer.getAttribute("", "a/c[9]", Integer.class));
			assertEquals((Integer)5, writer.getAttribute("", "a/c[5]", Integer.class));

			/* remove multiple, none  present */
			writer.setAttribute("", "a/b/c", 100);
			writer.setAttribute("", "a/b/d", "test");
			writer.setAttribute("", "a/c[9]", 10);
			writer.setAttribute("", "a/c[5]", 5);

			assertFalse(writer.removeAttributes("", Arrays.asList("X/b/c", "Z/b/d", "a/x[5]")));
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			assertEquals("test", writer.getAttribute("", "a/b/d", String.class));
			assertEquals(10, writer.getAttribute("", "a/c", JsonArray.class).size());
			assertEquals((Integer)10, writer.getAttribute("", "a/c[9]", Integer.class));
			assertEquals((Integer)5, writer.getAttribute("", "a/c[5]", Integer.class));

			/* Test path normalization */
			writer.setAttribute("", "a/b/c", 100);
			assertEquals((Integer)100, writer.getAttribute("", "a/b/c", Integer.class));
			assertEquals((Integer)100, writer.removeAttribute("", "a////b/x/../c", Integer.class));
			assertNull(writer.getAttribute("", "a/b/c", Integer.class));

			writer.createGroup("foo");
			writer.setAttribute("foo", "a", 100);
			writer.removeAttribute("foo", "a");
			assertNull(writer.getAttribute("foo", "a", Integer.class));
		}
	}

	@Test
	public void testRemoveContainer() throws IOException, URISyntaxException {

		final String location = tempN5Location();
		try (final N5Writer n5 = createTempN5Writer(location)) {
			try (N5Reader n5Reader = createN5Reader(location)) {
				assertNotNull(n5Reader);
			}
			assertTrue(n5.remove());
			assertThrows(Exception.class, () -> createN5Reader(location).close());
		}
		assertThrows(Exception.class, () -> createN5Reader(location).close());
	}

	@Test
	public void testUri() throws IOException, URISyntaxException {

		try (final N5Writer writer = createTempN5Writer()) {
			try (final N5Reader reader = createN5Reader(writer.getURI().toString())) {
				assertEquals(writer.getURI(), reader.getURI());
			}
		}
	}

	@Test
	public void testRemoveGroup() {

		try (final N5Writer n5 = createTempN5Writer()) {
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT64, new RawCompression());
			n5.remove(groupName);
			assertFalse("Group still exists", n5.exists(groupName));
		}

	}

	@Test
	public void testList() {

		try (final N5Writer listN5 = createTempN5Writer()) {
			listN5.createGroup(groupName);
			for (final String subGroup : subGroupNames)
				listN5.createGroup(groupName + "/" + subGroup);

			final String[] groupsList = listN5.list(groupName);
			Arrays.sort(groupsList);

			assertArrayEquals(subGroupNames, groupsList);

			// test listing the root group ("" and "/" should give identical results)
			assertArrayEquals(new String[]{"test"}, listN5.list(""));
			assertArrayEquals(new String[]{"test"}, listN5.list("/"));

			// calling list on a non-existant group throws an exception
			assertThrows(N5Exception.class, () -> listN5.list("this-group-does-not-exist"));

		}
	}

	@Test
	public void testDeepList() throws ExecutionException, InterruptedException {

		try (final N5Writer n5 = createTempN5Writer()) {

			n5.createGroup(groupName);
			for (final String subGroup : subGroupNames)
				n5.createGroup(groupName + "/" + subGroup);

			final List<String> groupsList = Arrays.asList(n5.deepList("/"));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", groupsList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", Arrays.asList(n5.deepList("")).contains(groupName.replaceFirst("/", "") + "/" + subGroup));

			final DatasetAttributes datasetAttributes = new DatasetAttributes(dimensions, blockSize, DataType.UINT64);
			final LongArrayDataBlock dataBlock = new LongArrayDataBlock(blockSize, new long[]{0, 0, 0}, new long[blockNumElements]);
			n5.createDataset(datasetName, datasetAttributes);
			n5.writeBlock(datasetName, datasetAttributes, dataBlock);

			final List<String> datasetList = Arrays.asList(n5.deepList("/"));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetList.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			assertTrue("deepList contents", datasetList.contains(datasetName.replaceFirst("/", "")));
			assertFalse("deepList stops at datasets", datasetList.contains(datasetName + "/0"));

			final List<String> datasetList2 = Arrays.asList(n5.deepList(""));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetList2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			assertTrue("deepList contents", datasetList2.contains(datasetName.replaceFirst("/", "")));
			assertFalse("deepList stops at datasets", datasetList2.contains(datasetName + "/0"));

			final String prefix = "/test";
			final List<String> datasetList3 = Arrays.asList(n5.deepList(prefix));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetList3.contains("group/" + subGroup));
			assertTrue("deepList contents", datasetList3.contains(datasetName.replaceFirst(prefix + "/", "")));

			// parallel deepList tests
			final List<String> datasetListP = Arrays.asList(n5.deepList("/", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetListP.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			assertTrue("deepList contents", datasetListP.contains(datasetName.replaceFirst("/", "")));
			assertFalse("deepList stops at datasets", datasetListP.contains(datasetName + "/0"));

			final List<String> datasetListP2 = Arrays.asList(n5.deepList("", Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetListP2.contains(groupName.replaceFirst("/", "") + "/" + subGroup));
			assertTrue("deepList contents", datasetListP2.contains(datasetName.replaceFirst("/", "")));
			assertFalse("deepList stops at datasets", datasetListP2.contains(datasetName + "/0"));

			final List<String> datasetListP3 = Arrays.asList(n5.deepList(prefix, Executors.newFixedThreadPool(2)));
			for (final String subGroup : subGroupNames)
				assertTrue("deepList contents", datasetListP3.contains("group/" + subGroup));
			assertTrue("deepList contents", datasetListP3.contains(datasetName.replaceFirst(prefix + "/", "")));
			assertFalse("deepList stops at datasets", datasetListP3.contains(datasetName + "/0"));

			// test filtering
			final Predicate<String> isCalledDataset = d -> d.endsWith("/dataset");
			final Predicate<String> isBorC = d -> d.matches(".*/[bc]$");

			final List<String> datasetListFilter1 = Arrays.asList(n5.deepList(prefix, isCalledDataset));
			assertTrue(
					"deepList filter \"dataset\"",
					datasetListFilter1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilter2 = Arrays.asList(n5.deepList(prefix, isBorC));
			assertTrue(
					"deepList filter \"b or c\"",
					datasetListFilter2.stream().map(x -> prefix + x).allMatch(isBorC));

			final List<String> datasetListFilterP1 =
					Arrays.asList(n5.deepList(prefix, isCalledDataset, Executors.newFixedThreadPool(2)));
			assertTrue(
					"deepList filter \"dataset\"",
					datasetListFilterP1.stream().map(x -> prefix + x).allMatch(isCalledDataset));

			final List<String> datasetListFilterP2 =
					Arrays.asList(n5.deepList(prefix, isBorC, Executors.newFixedThreadPool(2)));
			assertTrue(
					"deepList filter \"b or c\"",
					datasetListFilterP2.stream().map(x -> prefix + x).allMatch(isBorC));

			// test dataset filtering
			final List<String> datasetListFilterD = Arrays.asList(n5.deepListDatasets(prefix));
			assertTrue(
					"deepListDataset",
					datasetListFilterD.size() == 1 && (prefix + "/" + datasetListFilterD.get(0)).equals(datasetName));
			assertArrayEquals(
					datasetListFilterD.toArray(),
					n5.deepList(prefix, n5::datasetExists));

			final List<String> datasetListFilterDandBC = Arrays.asList(n5.deepListDatasets(prefix, isBorC));
			assertEquals("deepListDatasetFilter", 0, datasetListFilterDandBC.size());
			assertArrayEquals(
					datasetListFilterDandBC.toArray(),
					n5.deepList(prefix, a -> n5.datasetExists(a) && isBorC.test(a)));

			final List<String> datasetListFilterDP =
					Arrays.asList(n5.deepListDatasets(prefix, Executors.newFixedThreadPool(2)));
			assertTrue(
					"deepListDataset Parallel",
					datasetListFilterDP.size() == 1 && (prefix + "/" + datasetListFilterDP.get(0)).equals(datasetName));
			assertArrayEquals(
					datasetListFilterDP.toArray(),
					n5.deepList(prefix, n5::datasetExists, Executors.newFixedThreadPool(2)));

			final List<String> datasetListFilterDandBCP =
					Arrays.asList(n5.deepListDatasets(prefix, isBorC, Executors.newFixedThreadPool(2)));
			assertEquals("deepListDatasetFilter Parallel", 0, datasetListFilterDandBCP.size());
			assertArrayEquals(
					datasetListFilterDandBCP.toArray(),
					n5.deepList(prefix, a -> n5.datasetExists(a) && isBorC.test(a), Executors.newFixedThreadPool(2)));
		}
	}

	@Test
	public void testExists() {

		final String groupName2 = groupName + "-2";
		final String datasetName2 = datasetName + "-2";
		final String notExists = groupName + "-notexists";
		try (N5Writer n5 = createTempN5Writer()) {
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, new RawCompression());
			assertTrue(n5.exists(datasetName2));
			assertTrue(n5.datasetExists(datasetName2));

			n5.createGroup(groupName2);
			assertTrue(n5.exists(groupName2));
			assertFalse(n5.datasetExists(groupName2));

			assertFalse(n5.exists(notExists));
			assertFalse(n5.datasetExists(notExists));

		}
	}

	@Test
	public void testListAttributes() {

		try (N5Writer n5 = createTempN5Writer()) {
			final String groupName2 = groupName + "-2";
			final String datasetName2 = datasetName + "-2";
			n5.createDataset(datasetName2, dimensions, blockSize, DataType.UINT64, new RawCompression());
			n5.setAttribute(datasetName2, "attr1", new double[]{1.1, 2.1, 3.1});
			n5.setAttribute(datasetName2, "attr2", new String[]{"a", "b", "c"});
			n5.setAttribute(datasetName2, "attr3", 1.1);
			n5.setAttribute(datasetName2, "attr4", "a");
			n5.setAttribute(datasetName2, "attr5", new long[]{1, 2, 3});
			n5.setAttribute(datasetName2, "attr6", 1);
			n5.setAttribute(datasetName2, "attr7", new double[]{1, 2, 3.1});
			n5.setAttribute(datasetName2, "attr8", new Object[]{"1", 2, 3.1});

			Map<String, Class<?>> attributesMap = n5.listAttributes(datasetName2);
			assertEquals(attributesMap.get("attr1"), double[].class);
			assertEquals(attributesMap.get("attr2"), String[].class);
			assertEquals(attributesMap.get("attr3"), double.class);
			assertEquals(attributesMap.get("attr4"), String.class);
			assertEquals(attributesMap.get("attr5"), long[].class);
			assertEquals(attributesMap.get("attr6"), long.class);
			assertEquals(attributesMap.get("attr7"), double[].class);
			assertEquals(attributesMap.get("attr8"), Object[].class);

			n5.createGroup(groupName2);
			n5.setAttribute(groupName2, "attr1", new double[]{1.1, 2.1, 3.1});
			n5.setAttribute(groupName2, "attr2", new String[]{"a", "b", "c"});
			n5.setAttribute(groupName2, "attr3", 1.1);
			n5.setAttribute(groupName2, "attr4", "a");
			n5.setAttribute(groupName2, "attr5", new long[]{1, 2, 3});
			n5.setAttribute(groupName2, "attr6", 1);
			n5.setAttribute(groupName2, "attr7", new double[]{1, 2, 3.1});
			n5.setAttribute(groupName2, "attr8", new Object[]{"1", 2, 3.1});

			attributesMap = n5.listAttributes(groupName2);
			assertEquals(attributesMap.get("attr1"), double[].class);
			assertEquals(attributesMap.get("attr2"), String[].class);
			assertEquals(attributesMap.get("attr3"), double.class);
			assertEquals(attributesMap.get("attr4"), String.class);
			assertEquals(attributesMap.get("attr5"), long[].class);
			assertEquals(attributesMap.get("attr6"), long.class);
			assertEquals(attributesMap.get("attr7"), double[].class);
			assertEquals(attributesMap.get("attr8"), Object[].class);
		}

	}

	@Test
	public void testVersion() throws NumberFormatException, IOException, URISyntaxException {

		try (final N5Writer writer = createTempN5Writer()) {

			final Version n5Version = writer.getVersion();

			assertEquals(n5Version, N5Reader.VERSION);

			final Version incompatibleVersion = new Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, incompatibleVersion.toString());
			final Version version = writer.getVersion();
			assertFalse(N5Reader.VERSION.isCompatible(version));

			assertThrows(N5Exception.N5IOException.class, () -> createTempN5Writer(writer.getURI().toString()));

			final Version compatibleVersion = new Version(N5Reader.VERSION.getMajor(), N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, compatibleVersion.toString());
		}

	}

	@Test
	public void testReaderCreation() throws IOException, URISyntaxException {

		final String location;
		N5Writer removeMe = null;
		try (N5Writer writer = createN5Writer()) {
			removeMe = writer;
			location = writer.getURI().toString();

			try (N5Reader n5r = createN5Reader(location)) {
				assertNotNull(n5r);
			}

			// existing directory without attributes is okay;
			// Remove and create to remove attributes store
			writer.removeAttribute("/", "/");
			try (N5Reader na = createN5Reader(location)) {
				assertNotNull(na);
			}

			// existing location with attributes, but no version
			writer.removeAttribute("/", "/");
			writer.setAttribute("/", "mystring", "ms");
			try (N5Reader wa = createN5Reader(location)) {
				assertNotNull(wa);
			}

			// existing directory with incompatible version should fail
			writer.removeAttribute("/", "/");
			final String invalidVersion = new Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch()).toString();
			writer.setAttribute("/", N5Reader.VERSION_KEY, invalidVersion);
			assertThrows("Incompatible version throws error", N5Exception.class, () -> {
				try (final N5Reader ignored = createN5Reader(location)) {
					/*Only try with resource to ensure `close()` is called.*/
				}
			});
		} finally {
			removeMe.remove();
		}

		// non-existent location should fail
		assertThrows("Non-existent location throws error", N5Exception.N5IOException.class,
				() -> {
					try (N5Reader test = createN5Reader(location)) {
						test.list("/");
					}
				});
	}

	@Test
	public void testDelete() {

		try (N5Writer n5 = createTempN5Writer()) {
			final String datasetName = AbstractN5Test.datasetName + "-test-delete";
			n5.createDataset(datasetName, dimensions, blockSize, DataType.UINT8, new RawCompression());
			final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
			final long[] position1 = {0, 0, 0};
			final long[] position2 = {0, 1, 2};

			// no blocks should exist to begin with
			assertTrue(testDeleteIsBlockDeleted(n5.readBlock(datasetName, attributes, position1)));
			assertTrue(testDeleteIsBlockDeleted(n5.readBlock(datasetName, attributes, position2)));

			final ByteArrayDataBlock dataBlock = new ByteArrayDataBlock(blockSize, position1, byteBlock);
			n5.writeBlock(datasetName, attributes, dataBlock);

			// block should exist at position1 but not at position2
			final DataBlock<?> readBlock = n5.readBlock(datasetName, attributes, position1);
			assertNotNull(readBlock);
			assertTrue(readBlock instanceof ByteArrayDataBlock);
			assertArrayEquals(byteBlock, ((ByteArrayDataBlock)readBlock).getData());
			assertTrue(testDeleteIsBlockDeleted(n5.readBlock(datasetName, attributes, position2)));

			// deletion should report true in all cases
			assertTrue(n5.deleteBlock(datasetName, position1));
			assertTrue(n5.deleteBlock(datasetName, position1));
			assertTrue(n5.deleteBlock(datasetName, position2));

			// no block should exist anymore
			assertTrue(testDeleteIsBlockDeleted(n5.readBlock(datasetName, attributes, position1)));
			assertTrue(testDeleteIsBlockDeleted(n5.readBlock(datasetName, attributes, position2)));
		}
	}

	protected boolean testDeleteIsBlockDeleted(final DataBlock<?> dataBlock) {

		return dataBlock == null;
	}

	public static class TestData<T> {

		public String groupPath;
		public String attributePath;
		public T attributeValue;
		public Class<T> attributeClass;

		@SuppressWarnings("unchecked")
		public TestData(final String groupPath, final String key, final T attributeValue) {

			this.groupPath = groupPath;
			this.attributePath = key;
			this.attributeValue = attributeValue;
			this.attributeClass = (Class<T>)attributeValue.getClass();
		}
	}

	protected static void addAndTest(final N5Writer writer, final ArrayList<TestData<?>> existingTests, final TestData<?> testData) {
		/* test a new value on existing path */
		writer.setAttribute(testData.groupPath, testData.attributePath, testData.attributeValue);
		assertEquals(testData.attributeValue, writer.getAttribute(testData.groupPath, testData.attributePath, testData.attributeClass));
		assertEquals(testData.attributeValue,
				writer.getAttribute(testData.groupPath, testData.attributePath, TypeToken.get(testData.attributeClass).getType()));

		/* previous values should still be there, but we remove first if the test we just added overwrites. */
		existingTests.removeIf(test -> {
			try {
				final String normalizedTestKey = N5URI.from(null, "", test.attributePath).normalizeAttributePath().replaceAll("^/", "");
				final String normalizedTestDataKey = N5URI.from(null, "", testData.attributePath).normalizeAttributePath().replaceAll("^/", "");
				return normalizedTestKey.equals(normalizedTestDataKey);
			} catch (final URISyntaxException e) {
				throw new RuntimeException(e);
			}
		});
		runTests(writer, existingTests);
		existingTests.add(testData);
	}

	protected static void runTests(final N5Writer writer, final ArrayList<TestData<?>> existingTests) {

		for (final TestData<?> test : existingTests) {
			assertEquals(test.attributeValue, writer.getAttribute(test.groupPath, test.attributePath, test.attributeClass));
			assertEquals(test.attributeValue, writer.getAttribute(test.groupPath, test.attributePath, TypeToken.get(test.attributeClass).getType()));
		}
	}

	@Test
	public void testAttributePaths() {

		try (final N5Writer writer = createTempN5Writer()) {

			final String testGroup = "test";
			writer.createGroup(testGroup);

			final ArrayList<TestData<?>> existingTests = new ArrayList<>();

			/* Test a new value by path */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/b/c/key1", "value1"));
			/* test a new value on existing path */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/b/key2", "value2"));
			/* test replacing an existing value */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/b/c/key1", "new_value1"));

			/* Test a new value with arrays */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array[0]/b/c/key1", "array_value1"));
			/* test replacing an existing value */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array[0]/b/c/key1", "new_array_value1"));
			/* test a new value on existing path with arrays */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array[0]/d[3]/key2", "array_value2"));
			/* test a new value on existing path with nested arrays */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array[1][2]/[3]key2", "array2_value2"));
			/* test with syntax variants */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array[1][2]/[3]key2", "array3_value3"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "array[1]/[2][3]/key2", "array3_value4"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/array/[1]/[2]/[3]/key2", "array3_value5"));
			/* test with whitespace*/
			addAndTest(writer, existingTests, new TestData<>(testGroup, " ", "space"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "\n", "newline"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "\t", "tab"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "\r\n", "windows_newline"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, " \n\t \t \n \r\n\r\n", "mixed"));
			/* test URI encoded characters inside square braces */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[ ]", "space"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[\n]", "newline"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[\t]", "tab"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[\r\n]", "windows_newline"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[ ][\n][\t][ \t \n \r\n][\r\n]", "mixed"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[ ][\\n][\\t][ \\t \\n \\r\\n][\\r\\n]", "mixed"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "[\\]", "backslash"));

			/* Non String tests */

			addAndTest(writer, existingTests, new TestData<>(testGroup, "/an/integer/test", 1));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/double/test", 1.0));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/float/test", 1.0F));
			final TestData<Boolean> booleanTest = new TestData<>(testGroup, "/a/boolean/test", true);
			addAndTest(writer, existingTests, booleanTest);

			/* overwrite structure*/
			existingTests.remove(booleanTest);
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/a/boolean[2]/test", true));

			/* Fill an array with number */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/double_array[5]", 5.0));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/double_array[1]", 1.0));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/double_array[2]", 2.0));

			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/double_array[4]", 4.0));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/double_array[0]", 0.0));

			/* We intentionally skipped index 3, it should be `0` */
			assertEquals((Integer)0, writer.getAttribute(testGroup, "/filled/double_array[3]", Integer.class));

			/* Fill an array with Object */
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/string_array[5]", "f"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/string_array[1]", "b"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/string_array[2]", "c"));

			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/string_array[4]", "e"));
			addAndTest(writer, existingTests, new TestData<>(testGroup, "/filled/string_array[0]", "a"));

			/* We intentionally skipped index 3, but it should have been pre-populated with JsonNull */
			assertEquals(JsonNull.INSTANCE, writer.getAttribute(testGroup, "/filled/string_array[3]", JsonNull.class));

			/* Ensure that escaping does NOT interpret the json path structure, but rather it adds the keys opaquely*/
			final HashMap<String, Object> testAttributes = new HashMap<>();
			testAttributes.put("\\/z\\/y\\/x", 10);
			testAttributes.put("q\\/r\\/t", 11);
			testAttributes.put("\\/l\\/m\\[10]\\/n", 12);
			testAttributes.put("\\/", 13);
			/* intentionally the same as above, but this time it should be added as an opaque key*/
			testAttributes.put("\\/a\\/b/key2", "value2");
			writer.setAttributes(testGroup, testAttributes);

			assertEquals((Integer)10, writer.getAttribute(testGroup, "\\/z\\/y\\/x", Integer.class));
			assertEquals((Integer)11, writer.getAttribute(testGroup, "q\\/r\\/t", Integer.class));
			assertEquals((Integer)12, writer.getAttribute(testGroup, "\\/l\\/m\\[10]\\/n", Integer.class));
			assertEquals((Integer)13, writer.getAttribute(testGroup, "\\/", Integer.class));

			/* We are passing a different type for the same key ("/").
			 * This means it will try ot grab the exact match first, but then fail, and continuat on
			 * to try and grab the value as a json structure. I should grab the root, and match the empty string case */
			assertEquals(writer.getAttribute(testGroup, "", JsonObject.class), writer.getAttribute(testGroup, "/", JsonObject.class));

			/* Lastly, ensure grabing nonsense results in an exception */
			assertNull(writer.getAttribute(testGroup, "/this/key/does/not/exist", Object.class));

			writer.remove(testGroup);
		}
	}

	@Test
	public void testAttributePathEscaping() {

		final JsonObject emptyObj = new JsonObject();

		final String slashKey = "/";
		final String abcdefKey = "abc/def";
		final String zeroKey = "[0]";
		final String bracketsKey = "]] [] [[";
		final String doubleBracketsKey = "[[2][33]]";
		final String doubleBackslashKey = "\\\\\\\\"; //Evaluates to `\\` through java and json

		final String dataString = "dataString";
		final String rootSlash = jsonKeyVal(slashKey, dataString);
		final String abcdef = jsonKeyVal(abcdefKey, dataString);
		final String zero = jsonKeyVal(zeroKey, dataString);
		final String brackets = jsonKeyVal(bracketsKey, dataString);
		final String doubleBrackets = jsonKeyVal(doubleBracketsKey, dataString);
		final String doubleBackslash = jsonKeyVal(doubleBackslashKey, dataString);

		try (N5Writer n5 = createTempN5Writer()) {

			// "/" as key
			String grp = "a";
			n5.createGroup(grp);
			n5.setAttribute(grp, "\\/", dataString);
			assertEquals(dataString, n5.getAttribute(grp, "\\/", String.class));
			String jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(rootSlash));

			// "abc/def" as key
			grp = "b";
			n5.createGroup(grp);
			n5.setAttribute(grp, "abc\\/def", dataString);
			assertEquals(dataString, n5.getAttribute(grp, "abc\\/def", String.class));
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(abcdef));

			// "[0]"  as a key
			grp = "c";
			n5.createGroup(grp);
			n5.setAttribute(grp, "\\[0]", dataString);
			assertEquals(dataString, n5.getAttribute(grp, "\\[0]", String.class));
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(zero));

			// "]] [] [["  as a key
			grp = "d";
			n5.createGroup(grp);
			n5.setAttribute(grp, bracketsKey, dataString);
			assertEquals(dataString, n5.getAttribute(grp, bracketsKey, String.class));
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(brackets));

			// "[[2][33]]"
			grp = "e";
			n5.createGroup(grp);
			n5.setAttribute(grp, "[\\[2]\\[33]]", dataString);
			assertEquals(dataString, n5.getAttribute(grp, "[\\[2]\\[33]]", String.class));
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(doubleBrackets));

			// "\\" as key
			grp = "f";
			n5.createGroup(grp);
			n5.setAttribute(grp, "\\\\", dataString);
			assertEquals(dataString, n5.getAttribute(grp, "\\\\", String.class));
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertTrue(jsonContents.contains(doubleBackslash));

			// clear
			n5.setAttribute(grp, "/", emptyObj);
			jsonContents = n5.getAttribute(grp, "/", String.class);
			assertFalse(jsonContents.contains(doubleBackslash));

		}
	}

	/*
	 * For readability above
	 */
	private String jsonKeyVal(final String key, final String val) {

		return String.format("\"%s\":\"%s\"", key, val);
	}

	@Test
	public void
	testRootLeaves() {

		/* Test retrieving non-JsonObject root leaves */
		try (final N5Writer n5 = createTempN5Writer()) {
			n5.createGroup(groupName);
			n5.setAttribute(groupName, "/", "String");

			final JsonElement stringPrimitive = n5.getAttribute(groupName, "/", JsonElement.class);
			assertTrue(stringPrimitive.isJsonPrimitive());
			assertEquals("String", stringPrimitive.getAsString());
			n5.setAttribute(groupName, "/", 0);
			final JsonElement intPrimitive = n5.getAttribute(groupName, "/", JsonElement.class);
			assertTrue(intPrimitive.isJsonPrimitive());
			assertEquals(0, intPrimitive.getAsInt());
			n5.setAttribute(groupName, "/", true);
			final JsonElement booleanPrimitive = n5.getAttribute(groupName, "/", JsonElement.class);
			assertTrue(booleanPrimitive.isJsonPrimitive());
			assertTrue(booleanPrimitive.getAsBoolean());
			n5.setAttribute(groupName, "/", null);
			final JsonElement jsonNull = n5.getAttribute(groupName, "/", JsonElement.class);
			assertTrue(jsonNull.isJsonNull());
			assertEquals(JsonNull.INSTANCE, jsonNull);
			n5.setAttribute(groupName, "[5]", "array");
			final JsonElement rootJsonArray = n5.getAttribute(groupName, "/", JsonElement.class);
			assertTrue(rootJsonArray.isJsonArray());
			final JsonArray rootArray = rootJsonArray.getAsJsonArray();
			assertEquals("array", rootArray.get(5).getAsString());
			assertEquals(JsonNull.INSTANCE, rootArray.get(3));
			assertThrows(IndexOutOfBoundsException.class, () -> rootArray.get(10));
		}

		/* Test with new root's each time */
		final ArrayList<TestData<?>> tests = new ArrayList<>();
		tests.add(new TestData<>(groupName, "", "empty_root"));
		tests.add(new TestData<>(groupName, "/", "replace_empty_root"));
		tests.add(new TestData<>(groupName, "[0]", "array_root"));

		for (final TestData<?> testData : tests) {
			try (final N5Writer writer = createTempN5Writer()) {
				writer.createGroup(testData.groupPath);
				writer.setAttribute(testData.groupPath, testData.attributePath, testData.attributeValue);
				assertEquals(testData.attributeValue, writer.getAttribute(testData.groupPath, testData.attributePath, testData.attributeClass));
				assertEquals(testData.attributeValue,
						writer.getAttribute(testData.groupPath, testData.attributePath, TypeToken.get(testData.attributeClass).getType()));
			}
		}

		/* Test with replacing an existing root-leaf */
		tests.clear();
		tests.add(new TestData<>(groupName, "", "empty_root"));
		tests.add(new TestData<>(groupName, "/", "replace_empty_root"));
		tests.add(new TestData<>(groupName, "[0]", "array_root"));

		try (final N5Writer writer = createTempN5Writer()) {
			writer.createGroup(groupName);
			for (final TestData<?> testData : tests) {
				writer.setAttribute(testData.groupPath, testData.attributePath, testData.attributeValue);
				assertEquals(testData.attributeValue, writer.getAttribute(testData.groupPath, testData.attributePath, testData.attributeClass));
				assertEquals(testData.attributeValue,
						writer.getAttribute(testData.groupPath, testData.attributePath, TypeToken.get(testData.attributeClass).getType()));
			}
		}

		/* Test with replacing an existing root non-leaf*/
		tests.clear();
		final TestData<Integer> rootAsObject = new TestData<>(groupName, "/some/non/leaf[3]/structure", 100);
		final TestData<Integer> rootAsPrimitive = new TestData<>(groupName, "", 200);
		final TestData<Integer> rootAsArray = new TestData<>(groupName, "/", 300);
		tests.add(rootAsPrimitive);
		tests.add(rootAsArray);
		try (final N5Writer writer = createTempN5Writer()) {
			writer.createGroup(groupName);
			for (final TestData<?> test : tests) {
				/* Set the root as Object*/
				writer.setAttribute(rootAsObject.groupPath, rootAsObject.attributePath, rootAsObject.attributeValue);
				assertEquals(rootAsObject.attributeValue,
						writer.getAttribute(rootAsObject.groupPath, rootAsObject.attributePath, rootAsObject.attributeClass));

				/* Override the root with something else */
				writer.setAttribute(test.groupPath, test.attributePath, test.attributeValue);
				/* Verify original root is gone */
				assertNull(writer.getAttribute(rootAsObject.groupPath, rootAsObject.attributePath, rootAsObject.attributeClass));
				/* verify new root exists */
				assertEquals(test.attributeValue, writer.getAttribute(test.groupPath, test.attributePath, test.attributeClass));
			}
		}
	}

	@Test
	public void testWriterSeparation() {

		try (N5Writer writer1 = createTempN5Writer()) {
			try (N5Writer writer2 = createTempN5Writer()) {

				assertTrue(writer1.exists("/"));
				assertTrue(writer2.exists("/"));

				assertTrue(writer1.remove());
				assertTrue(writer2.exists("/"));
				assertFalse(writer1.exists("/"));

				assertTrue(writer2.remove());
				assertFalse(writer2.exists("/"));
			}
		}
	}
}
