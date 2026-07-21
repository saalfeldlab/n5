package org.janelia.saalfeldlab.n5.codec.dataset;

import static org.junit.Assert.assertArrayEquals;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.junit.Test;

public class ScaleOffsetCodecTests {

	@Test
	@SuppressWarnings("unchecked")
	public void testScaleOffsetRoundTripFloat64() {

		final double scale = 4.0;
		final double offset = 10.0;
		final ScaleOffsetCodec<double[]> codec = new ScaleOffsetCodec<>(DataType.FLOAT64, scale, offset);

		final double[] original = {10.0, 11.0, 12.5, 9.75, -3.0};
		final DataBlock<double[]> block = (DataBlock<double[]>)DataType.FLOAT64.createDataBlock(
				new int[]{original.length}, new long[]{0});
		System.arraycopy(original, 0, block.getData(), 0, original.length);

		// encode: out = (in - offset) * scale
		final DataBlock<double[]> encoded = codec.encode(block);
		final double[] expected = new double[original.length];
		for (int i = 0; i < original.length; i++)
			expected[i] = (original[i] - offset) * scale;
		assertArrayEquals(expected, encoded.getData(), 0.0);

		// decode: out = (in / scale) + offset, recovering the original
		final DataBlock<double[]> decoded = codec.decode(encoded);
		assertArrayEquals(original, decoded.getData(), 1e-12);

		// grid position and size are preserved
		assertArrayEquals(block.getGridPosition(), encoded.getGridPosition());
		assertArrayEquals(block.getSize(), encoded.getSize());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testScaleOffsetFloat32() {

		final ScaleOffsetCodec<float[]> codec = new ScaleOffsetCodec<>(DataType.FLOAT32, 2.0, 1.0);

		final float[] original = {1.0f, 2.0f, 3.0f};
		final DataBlock<float[]> block = (DataBlock<float[]>)DataType.FLOAT32.createDataBlock(
				new int[]{original.length}, new long[]{0});
		System.arraycopy(original, 0, block.getData(), 0, original.length);

		final DataBlock<float[]> encoded = codec.encode(block);
		assertArrayEquals(new float[]{0.0f, 2.0f, 4.0f}, encoded.getData(), 0.0f);
		assertArrayEquals(original, codec.decode(encoded).getData(), 1e-6f);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testScaleOffsetIdentityDefaults() {

		// scale=1, offset=0 is a no-op
		final ScaleOffsetCodec<double[]> codec = new ScaleOffsetCodec<>(DataType.FLOAT64, 1.0, 0.0);

		final double[] original = {-1.0, 0.0, 42.0};
		final DataBlock<double[]> block = (DataBlock<double[]>)DataType.FLOAT64.createDataBlock(
				new int[]{original.length}, new long[]{0});
		System.arraycopy(original, 0, block.getData(), 0, original.length);

		assertArrayEquals(original, codec.encode(block).getData(), 0.0);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testScaleOffsetUint8() {

		// unsigned bytes should be interpreted in [0, 255]
		final ScaleOffsetCodec<byte[]> codec = new ScaleOffsetCodec<>(DataType.UINT8, 1.0, 100.0);

		final byte[] original = {(byte)100, (byte)200, (byte)255};
		final DataBlock<byte[]> block = (DataBlock<byte[]>)DataType.UINT8.createDataBlock(
				new int[]{original.length}, new long[]{0});
		System.arraycopy(original, 0, block.getData(), 0, original.length);

		// encode: (in - 100) * 1  ->  0, 100, 155
		final DataBlock<byte[]> encoded = codec.encode(block);
		assertArrayEquals(new byte[]{0, (byte)100, (byte)155}, encoded.getData());

		// decode recovers the original unsigned values
		assertArrayEquals(original, codec.decode(encoded).getData());
	}
}
