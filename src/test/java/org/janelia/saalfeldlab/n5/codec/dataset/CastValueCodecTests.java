package org.janelia.saalfeldlab.n5.codec.dataset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
import org.junit.Test;

public class CastValueCodecTests {

	@SuppressWarnings("unchecked")
	private static <T> DataBlock<T> block(final DataType dataType, final int numElements) {

		return (DataBlock<T>)dataType.createDataBlock(new int[]{numElements}, new long[]{0});
	}

	private static DataBlock<double[]> float64Block(final double... values) {

		final DataBlock<double[]> block = block(DataType.FLOAT64, values.length);
		System.arraycopy(values, 0, block.getData(), 0, values.length);
		return block;
	}

	private static DataBlock<short[]> int16Block(final short... values) {

		final DataBlock<short[]> block = block(DataType.INT16, values.length);
		System.arraycopy(values, 0, block.getData(), 0, values.length);
		return block;
	}

	@Test
	public void testFloat64ToInt16RoundTrip() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, null);

		final DataBlock<short[]> encoded = codec.encode(float64Block(0.0, 1.0, -1.0, 1000.0, -32768.0, 32767.0));
		assertArrayEquals(new short[]{0, 1, -1, 1000, -32768, 32767}, encoded.getData());

		// decoding casts back to the source type exactly
		assertArrayEquals(new double[]{0.0, 1.0, -1.0, 1000.0, -32768.0, 32767.0},
				codec.decode(encoded).getData(), 0.0);

		// size and grid position are preserved
		assertArrayEquals(new int[]{6}, encoded.getSize());
		assertArrayEquals(new long[]{0}, encoded.getGridPosition());
	}

	@Test
	public void testRoundingNearestEvenTiesToEven() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, null);

		// ties round to the even neighbour
		assertArrayEquals(new short[]{0, 2, 2, 4, -2, -2},
				codec.encode(float64Block(0.5, 1.5, 2.5, 3.5, -1.5, -2.5)).getData());
	}

	@Test
	public void testRoundingTowardsZero() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.TOWARDS_ZERO, null);

		assertArrayEquals(new short[]{1, 1, -1, -1},
				codec.encode(float64Block(1.2, 1.8, -1.2, -1.8)).getData());
	}

	@Test
	public void testRoundingTowardsPositiveAndNegative() {

		final CastValueCodec<double[], short[]> up = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.TOWARDS_POSITIVE, null);
		assertArrayEquals(new short[]{2, 2, -1, -1},
				up.encode(float64Block(1.2, 1.8, -1.2, -1.8)).getData());

		final CastValueCodec<double[], short[]> down = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.TOWARDS_NEGATIVE, null);
		assertArrayEquals(new short[]{1, 1, -2, -2},
				down.encode(float64Block(1.2, 1.8, -1.2, -1.8)).getData());
	}

	@Test
	public void testRoundingNearestAwayFromZero() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_AWAY, null);

		// ties round away from zero, unlike nearest-even
		assertArrayEquals(new short[]{1, 2, 3, -1, -2, -3},
				codec.encode(float64Block(0.5, 1.5, 2.5, -0.5, -1.5, -2.5)).getData());
	}

	@Test
	public void testOutOfRangeErrorsByDefault() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, null);

		assertThrows(N5IOException.class, () -> codec.encode(float64Block(32768.0)));
		assertThrows(N5IOException.class, () -> codec.encode(float64Block(-32769.0)));
	}

	@Test
	public void testOutOfRangeClamp() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		assertArrayEquals(new short[]{32767, -32768, 32767, -32768},
				codec.encode(float64Block(32768.0, -32769.0, 1e9, -1e9)).getData());
	}

	@Test
	public void testOutOfRangeClampInfinities() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		assertArrayEquals(new short[]{32767, -32768},
				codec.encode(float64Block(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)).getData());
	}

	@Test
	public void testOutOfRangeWrapSigned() {

		final CastValueCodec<double[], byte[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT8, Rounding.NEAREST_EVEN, OutOfRange.WRAP);

		// 128 wraps to -128, 255 wraps to -1, 256 wraps to 0
		assertArrayEquals(new byte[]{-128, -1, 0, 1},
				codec.encode(float64Block(128.0, 255.0, 256.0, 257.0)).getData());

		// negative values wrap modularly too
		assertArrayEquals(new byte[]{127, 0},
				codec.encode(float64Block(-129.0, -256.0)).getData());
	}

	@Test
	public void testOutOfRangeWrapUnsigned() {

		final CastValueCodec<double[], byte[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.UINT8, Rounding.NEAREST_EVEN, OutOfRange.WRAP);

		// stored bits wrap modulo 256, and read back as unsigned
		final DataBlock<byte[]> encoded = codec.encode(float64Block(256.0, 257.0, -1.0));
		assertArrayEquals(new double[]{0.0, 1.0, 255.0}, codec.decode(encoded).getData(), 0.0);
	}

	@Test
	public void testNaNToIntegralIsAnError() {

		final CastValueCodec<double[], short[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		assertThrows(N5IOException.class, () -> codec.encode(float64Block(Double.NaN)));
	}

	@Test
	public void testNaNAndInfinityPropagateBetweenFloatTypes() {

		final CastValueCodec<double[], float[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.FLOAT32, Rounding.NEAREST_EVEN, null);

		final float[] encoded = codec.encode(
				float64Block(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)).getData();

		assertTrue(Float.isNaN(encoded[0]));
		assertEquals(Float.POSITIVE_INFINITY, encoded[1], 0.0f);
		assertEquals(Float.NEGATIVE_INFINITY, encoded[2], 0.0f);
	}

	@Test
	public void testSignedZeroIsPreserved() {

		final CastValueCodec<double[], float[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.FLOAT32, Rounding.NEAREST_EVEN, null);

		final float[] encoded = codec.encode(float64Block(-0.0, 0.0)).getData();
		assertEquals(Float.floatToIntBits(-0.0f), Float.floatToIntBits(encoded[0]));
		assertEquals(Float.floatToIntBits(0.0f), Float.floatToIntBits(encoded[1]));
	}

	@Test
	public void testFloat64ToFloat32OverflowErrorsByDefault() {

		final CastValueCodec<double[], float[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.FLOAT32, Rounding.NEAREST_EVEN, null);

		// finite as a double, but overflows the float32 range
		assertThrows(N5IOException.class, () -> codec.encode(float64Block(1e40)));
	}

	@Test
	public void testFloat64ToFloat32OverflowClampsToInfinity() {

		final CastValueCodec<double[], float[]> codec = new CastValueCodec<>(
				DataType.FLOAT64, DataType.FLOAT32, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		final float[] encoded = codec.encode(float64Block(1e40, -1e40)).getData();
		assertEquals(Float.POSITIVE_INFINITY, encoded[0], 0.0f);
		assertEquals(Float.NEGATIVE_INFINITY, encoded[1], 0.0f);
	}

	@Test
	public void testUnsignedSourceIsReadUnsigned() {

		// int16 -> uint8: decoding reads the unsigned byte back as 200, not -56
		final CastValueCodec<short[], byte[]> codec = new CastValueCodec<>(
				DataType.INT16, DataType.UINT8, Rounding.NEAREST_EVEN, null);

		final DataBlock<byte[]> encoded = codec.encode(int16Block((short)0, (short)200, (short)255));
		assertArrayEquals(new byte[]{0, (byte)200, (byte)255}, encoded.getData());
		assertArrayEquals(new short[]{0, 200, 255}, codec.decode(encoded).getData());
	}

	@Test
	public void testUint8SourceOutOfRangeForInt8Target() {

		final CastValueCodec<byte[], byte[]> codec = new CastValueCodec<>(
				DataType.UINT8, DataType.INT8, Rounding.NEAREST_EVEN, null);

		final DataBlock<byte[]> in = block(DataType.UINT8, 1);
		in.getData()[0] = (byte)200; // reads as 200, which does not fit int8

		assertThrows(N5IOException.class, () -> codec.encode(in));
	}

	@Test
	public void testScaleOffsetThenCastValuePipeline() {

		// the canonical lossy-float workflow: rescale, then narrow
		final ScaleOffsetCodec<double[]> scaleOffset = new ScaleOffsetCodec<>(DataType.FLOAT64, 100.0, 0.0);
		final CastValueCodec<double[], short[]> cast = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, null);

		final double[] original = {0.0, 0.125, 1.5, -2.25};

		// 0.125 * 100 = 12.5, which nearest-even rounds down to 12
		final DataBlock<short[]> encoded = cast.encode(scaleOffset.encode(float64Block(original)));
		assertArrayEquals(new short[]{0, 12, 150, -225}, encoded.getData());

		// the round trip is lossy exactly where rounding occurred
		final double[] decoded = scaleOffset.decode(cast.decode(encoded)).getData();
		assertArrayEquals(new double[]{0.0, 0.12, 1.5, -2.25}, decoded, 1e-12);
	}
}
