/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.codec.BytesCodecTests.BitShiftBytesCodec;
import org.janelia.saalfeldlab.n5.shard.DatasetAccess;
import org.janelia.saalfeldlab.n5.shard.DatasetAccessTest;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;
import org.janelia.saalfeldlab.n5.shard.TestPositionValueAccess;
import org.junit.Test;

public class BlockCodecTests {

	static Random random = new Random(12345);

	final int[] blockSize = {11, 7, 5};
	private final BitShiftBytesCodec shiftCodec = new BitShiftBytesCodec(3);
	private final GzipCompression compressor = new GzipCompression();
	private final DataCodecInfo[][] dataCodecInfos = new DataCodecInfo[][]{
			{}, // empty: "raw" compression
			{compressor},
			{shiftCodec},
			{shiftCodec, compressor}
	};

	private final DataType[] dataTypes = {
			DataType.INT8, DataType.UINT8,
			DataType.INT16, DataType.UINT16,
			DataType.INT32, DataType.UINT32,
			DataType.INT64, DataType.UINT64,
			DataType.FLOAT32, DataType.FLOAT64
	};

	@Test
	public void testN5BlockCodec() throws Exception {
		for (DataType dataType : dataTypes) {
			for (DataCodecInfo[] dataCodecInfo : dataCodecInfos) {

				final DatasetAttributes attributes = new DatasetAttributes(
						new long[]{32, 32, 32},
						blockSize,
						dataType,
						new N5BlockCodecInfo(),
						dataCodecInfo);

				testBlockCodecHelper(attributes);
			}
		}
	}

	@Test
	public void testRawBytesBlockCodec() throws Exception {
		// Test RawBlockCodecInfo codec with different byte orders and DataTypes
		final ByteOrder[] byteOrders = {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN};
		for (DataType dataType : dataTypes) {
			for (ByteOrder byteOrder : byteOrders) {
				for (DataCodecInfo[] codecs : dataCodecInfos) {

					final RawBlockCodecInfo codec = new RawBlockCodecInfo(byteOrder);
					final DatasetAttributes attributes = new DatasetAttributes(
							new long[]{32, 32, 32},
							blockSize,
							dataType,
							codec,
							codecs);

					testBlockCodecHelper(attributes);
				}
			}
		}
	}

	private <T> void testBlockCodecHelper(DatasetAttributes attributes) throws Exception {

		// TODO
//		final int[] blockSize = attributes.getBlockSize();
//		final DataType dataType = attributes.getDataType();
//		final long[] gridPosition = {3, 2, 1};
//
//		// Create appropriate data block based on type
//		DataBlock<T> originalBlock = ((DataBlock<T>)createRandomDataBlock(dataType, blockSize, gridPosition));
//		final BlockCodec<T> codec = attributes.getBlockCodec();
//
//		// Test encode/decode roundtrip
//		final ReadData encoded = codec.encode(originalBlock);
//		assertNotNull(encoded);
//
//		final DataBlock<?> decoded = codec.decode(encoded, gridPosition);
//		assertNotNull(decoded);
//
//		assertArrayEquals("Block size should match", blockSize, decoded.getSize());
//		assertArrayEquals("Grid position should match", gridPosition, decoded.getGridPosition());
//		assertDataEquals(originalBlock, decoded);
//		verifyCompatibleDataType(dataType, decoded);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEmptyBlock() throws Exception {
		// Test handling of empty blocks
		final int[] blockSize = {0, 0};
		final long[] gridPosition = {0, 0};
		final N5BlockCodecInfo blockCodecInfo = new N5BlockCodecInfo();
		final DatasetAccessTest.TestDatasetAttributes attributes = new DatasetAccessTest.TestDatasetAttributes(
				new long[]{64, 64},
				new int[]{8, 8},
				DataType.UINT8,
				blockCodecInfo,
				new RawCompression());

		final PositionValueAccess store = new TestPositionValueAccess();
		DatasetAccess access = attributes.datasetAccess();

		// Test encode/decode
		final ByteArrayDataBlock emptyBlock = new ByteArrayDataBlock(blockSize, gridPosition, new byte[0]);

		access.writeBlock(store, emptyBlock);
		final DataBlock<?> decoded = access.readBlock(store, gridPosition);

		assertEquals("Empty block should have 0 elements", 0, decoded.getNumElements());
	}

	@Test
	public void testEncodedSizeCalculation() throws Exception {

		// TODO

		// Test that encoded size calculations are correct
//		final int[] blockSize = {64, 64};
//		final DatasetAttributes n5ArrayAttrs = new DatasetAttributes(
//				new long[]{512, 512},
//				blockSize,
//				blockSize,
//				DataType.INT16,
//				new N5BlockCodecInfo());
//
//
//		final DatasetAttributes rawArrayAttrs = new DatasetAttributes(
//				new long[]{512, 512},
//				blockSize,
//				blockSize,
//				DataType.INT16,
//				new RawBlockCodecInfo());
//
//		// Calculate expected sizes
//		final long rawDataSize = blockSize[0] * blockSize[1] * 2; // INT16 has 2 bytes per element
//
//		// N5BlockCodecInfo adds a header
//		// the estimate of the encoded size
//		final long n5EncodedSize = n5ArrayAttrs.getBlockCodecInfo().encodedSize(rawDataSize);
//		assertTrue("N5 encoded size should be larger than raw size", n5EncodedSize > rawDataSize);
//
//		DataBlock<short[]> dataBlock = ((DataBlock<short[]>)createRandomDataBlock(n5ArrayAttrs.getDataType(), blockSize, new long[]{0, 0}));
//		ReadData n5EncodedDataBlock = n5ArrayAttrs.<short[]>getBlockCodec().encode(dataBlock);
//		assertEquals("N5 actual encoded size should equal estimated size", n5EncodedSize, n5EncodedDataBlock.length());
//
//		// RawBlockCodecInfo should not change size
//		final long rawEncodedSize = rawArrayAttrs.getBlockCodecInfo().encodedSize(rawDataSize);
//		assertEquals("Raw encoded size should equal input size", rawDataSize, rawEncodedSize);
//
//		ReadData rawEncodedDataBlock = rawArrayAttrs.<short[]>getBlockCodec().encode(dataBlock);
//		assertEquals("Raw actual encoded size should equal estimated size", rawEncodedSize, rawEncodedDataBlock.length());
	}

	private static DataBlock<?> createRandomDataBlock(DataType dataType, int[] blockSize, long[] gridPosition) {
		final int numElements = Arrays.stream(blockSize).reduce(1, (a, b) -> a * b);

		switch (dataType) {
			case INT8:
			case UINT8:
				byte[] uint8Data = new byte[numElements];
				for (int i = 0; i < numElements; i++) {
					uint8Data[i] = (byte) random.nextInt(256);
				}
				return new ByteArrayDataBlock(blockSize, gridPosition, uint8Data);

			case INT16:
			case UINT16:
				short[] uint16Data = new short[numElements];
				for (int i = 0; i < numElements; i++) {
					uint16Data[i] = (short) random.nextInt(65536);
				}
				return new ShortArrayDataBlock(blockSize, gridPosition, uint16Data);

			case INT32:
			case UINT32:
				int[] uint32Data = new int[numElements];
				for (int i = 0; i < numElements; i++) {
					uint32Data[i] = random.nextInt();
				}
				return new IntArrayDataBlock(blockSize, gridPosition, uint32Data);

			case INT64:
			case UINT64:
				long[] uint64Data = new long[numElements];
				for (int i = 0; i < numElements; i++) {
					uint64Data[i] = random.nextLong();
				}
				return new LongArrayDataBlock(blockSize, gridPosition, uint64Data);

			case FLOAT32:
				float[] floatData = new float[numElements];
				for (int i = 0; i < numElements; i++) {
					floatData[i] = random.nextFloat();
				}
				return new FloatArrayDataBlock(blockSize, gridPosition, floatData);

			case FLOAT64:
				double[] doubleData = new double[numElements];
				for (int i = 0; i < numElements; i++) {
					doubleData[i] = random.nextDouble();
				}
				return new DoubleArrayDataBlock(blockSize, gridPosition, doubleData);

			default:
				throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
	}

	private static void verifyCompatibleDataType(DataType expectedType, DataBlock<?> block) {

		Object data = block.getData();
		switch (expectedType) {
			case INT8:
			case UINT8:
				assertTrue("Expected byte array for " + expectedType, data instanceof byte[]);
				break;
			case INT16:
			case UINT16:
				assertTrue("Expected short array for " + expectedType, data instanceof short[]);
				break;
			case INT32:
			case UINT32:
				assertTrue("Expected int array for " + expectedType, data instanceof int[]);
				break;
			case INT64:
			case UINT64:
				assertTrue("Expected long array for " + expectedType, data instanceof long[]);
				break;
			case FLOAT32:
				assertTrue("Expected float array for " + expectedType, data instanceof float[]);
				break;
			case FLOAT64:
				assertTrue("Expected double array for " + expectedType, data instanceof double[]);
				break;
			default:
				throw new IllegalArgumentException("Unsupported data type: " + expectedType);
		}
	}

	private static void assertDataEquals(DataBlock<?> expected, DataBlock<?> actual) {

		Object expectedData = expected.getData();
		Object actualData = actual.getData();

		if (expectedData instanceof byte[]) {
			assertArrayEquals((byte[]) expectedData, (byte[]) actualData);
		} else if (expectedData instanceof short[]) {
			assertArrayEquals((short[]) expectedData, (short[]) actualData);
		} else if (expectedData instanceof int[]) {
			assertArrayEquals((int[]) expectedData, (int[]) actualData);
		} else if (expectedData instanceof long[]) {
			assertArrayEquals((long[]) expectedData, (long[]) actualData);
		} else if (expectedData instanceof float[]) {
			assertArrayEquals((float[]) expectedData, (float[]) actualData, 0.0f);
		} else if (expectedData instanceof double[]) {
			assertArrayEquals((double[]) expectedData, (double[]) actualData, 0.0);
		} else {
			throw new IllegalArgumentException("Unknown data type");
		}
	}


}
