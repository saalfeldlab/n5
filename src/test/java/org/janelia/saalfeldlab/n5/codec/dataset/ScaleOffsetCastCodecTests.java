package org.janelia.saalfeldlab.n5.codec.dataset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * Smoke tests for {@link ScaleOffsetCastCodec}, focused on the motivating case:
 * {@code float64} values mapped so {@code [-20, 20]} lands on {@code [-64, 64]}
 * of an {@code int8}, with everything outside {@code [-20, 20]} saturating at
 * {@code ±64}. Also checks that the fused codec agrees with the equivalent
 * four-codec chain it replaces.
 */
public class ScaleOffsetCastCodecTests {

	private static final double SCALE = 64.0 / 20.0; // 3.2

	@SuppressWarnings("unchecked")
	private static DataBlock<double[]> float64Block(final double... values) {

		final DataBlock<double[]> block = (DataBlock<double[]>)DataType.FLOAT64.createDataBlock(
				new int[]{values.length}, new long[]{0});
		System.arraycopy(values, 0, block.getData(), 0, values.length);
		return block;
	}

	private static ScaleOffsetCastCodec<double[], byte[]> clampToInt8() {

		return new ScaleOffsetCastCodec<>(
				DataType.FLOAT64, DataType.INT8, SCALE, 0.0, -64.0, 64.0, Rounding.NEAREST_EVEN);
	}

	@Test
	public void mapsBandLinearlyAndSaturatesTails() {

		//                                     below   edge  mid   zero  mid   edge  above
		final DataBlock<double[]> in = float64Block(-22.0, -20.0, -10.0, 0.0, 10.0, 20.0, 24.0);
		final DataBlock<byte[]> out = clampToInt8().encode(in);

		final byte[] expected = {-64, -64, -32, 0, 32, 64, 64};
		assertArrayEquals(expected, out.getData());
	}

	@Test
	public void decodeInvertsTheAffineMapWithinTheBand() {

		final DataBlock<double[]> in = float64Block(-20.0, -10.0, 0.0, 10.0, 20.0);
		final ScaleOffsetCastCodec<double[], byte[]> codec = clampToInt8();

		final double[] round = codec.decode(codec.encode(in)).getData();
		// values inside the band round-trip up to the quantization step (1/3.2)
		final double[] original = (double[])in.getData();
		for (int i = 0; i < original.length; i++)
			assertEquals(original[i], round[i], 0.5 / SCALE);
	}

	@Test
	public void nanNarrowsToZero() {

		// Math.min/max propagate NaN, which narrows to 0 for an integral target.
		final DataBlock<double[]> in = float64Block(Double.NaN);
		final byte[] out = clampToInt8().encode(in).getData();
		assertEquals(0, out[0]);
	}

	@Test
	public void infinitiesSaturateToTheBand() {

		final DataBlock<double[]> in = float64Block(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
		final byte[] out = clampToInt8().encode(in).getData();
		assertEquals(64, out[0]);
		assertEquals(-64, out[1]);
	}

	@Test
	public void agreesWithTheEquivalentFourCodecChain() {

		final double[] values = new double[512];
		final java.util.Random random = new java.util.Random(7777);
		for (int i = 0; i < values.length; i++)
			values[i] = -22.0 + random.nextDouble() * 46.0;

		final byte[] fused = clampToInt8().encode(float64Block(values)).getData();

		// int16 clamp domain: ±20 -> ±32767, then back down to [-64, 64]
		final double up = Short.MAX_VALUE / 20.0;
		final double down = 64.0 / Short.MAX_VALUE;
		final ScaleOffsetCodec<double[]> scaleUp = new ScaleOffsetCodec<>(DataType.FLOAT64, up, 0.0);
		final CastValueCodec<double[], short[]> clamp16 = new CastValueCodec<>(
				DataType.FLOAT64, DataType.INT16, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);
		final ScaleOffsetCodec<short[]> scaleDown = new ScaleOffsetCodec<>(DataType.INT16, down, 0.0);
		final CastValueCodec<short[], byte[]> cast8 = new CastValueCodec<>(
				DataType.INT16, DataType.INT8, Rounding.NEAREST_EVEN, null);

		final byte[] chain = cast8.encode(scaleDown.encode(clamp16.encode(scaleUp.encode(float64Block(values)))))
				.getData();

		// The chain truncates in its int16->int16 scale-down step, so allow a
		// one-code difference; the point is they agree to within rounding.
		int mismatches = 0;
		for (int i = 0; i < values.length; i++)
			if (Math.abs(fused[i] - chain[i]) > 1)
				mismatches++;

		assertEquals("fused and chain should agree to within one code", 0, mismatches);
	}

	// ------------------------------------------------------------------
	// ScaleOffsetCastCodecInfo: create(), defaults, validation, serialization
	// ------------------------------------------------------------------

	@Test
	public void createBuildsAWorkingCodec() {

		final ScaleOffsetCastCodecInfo info = new ScaleOffsetCastCodecInfo(
				DataType.INT8, SCALE, 0.0, -64.0, 64.0, Rounding.NEAREST_EVEN);

		@SuppressWarnings("unchecked")
		final ScaleOffsetCastCodec<double[], byte[]> codec =
				(ScaleOffsetCastCodec<double[], byte[]>)info.create(null, DataType.FLOAT64);

		final byte[] out = codec.encode(float64Block(-22.0, 0.0, 24.0)).getData();
		assertArrayEquals(new byte[]{-64, 0, 64}, out);
	}

	@Test
	public void clampBandDefaultsToTheTargetTypeRange() {

		// no clamp bounds given -> clamp to int8's full range [-128, 127]
		final ScaleOffsetCastCodecInfo info = new ScaleOffsetCastCodecInfo(
				DataType.INT8, 1.0, 0.0, null, null, null);

		@SuppressWarnings("unchecked")
		final ScaleOffsetCastCodec<double[], byte[]> codec =
				(ScaleOffsetCastCodec<double[], byte[]>)info.create(null, DataType.FLOAT64);

		// 200 saturates to 127, -200 to -128; 50 passes through
		final byte[] out = codec.encode(float64Block(200.0, 50.0, -200.0)).getData();
		assertArrayEquals(new byte[]{127, 50, -128}, out);
	}

	@Test
	public void clampBandOutsideTargetRangeIsRejected() {

		final ScaleOffsetCastCodecInfo tooHigh = new ScaleOffsetCastCodecInfo(
				DataType.INT8, 1.0, 0.0, -64.0, 200.0, null);
		assertThrows(N5Exception.class, () -> tooHigh.create(null, DataType.FLOAT64));

		final ScaleOffsetCastCodecInfo inverted = new ScaleOffsetCastCodecInfo(
				DataType.INT8, 1.0, 0.0, 64.0, -64.0, null);
		assertThrows(N5Exception.class, () -> inverted.create(null, DataType.FLOAT64));
	}

	@Test
	public void nonNumericalTypesAreRejected() {

		final ScaleOffsetCastCodecInfo info = new ScaleOffsetCastCodecInfo(
				DataType.STRING, 1.0, 0.0, null, null, null);
		assertThrows(N5Exception.class, () -> info.create(null, DataType.FLOAT64));
	}

	@Test
	public void serializationRoundTrips() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
		final Gson gson = gsonBuilder.create();

		final ScaleOffsetCastCodecInfo original = new ScaleOffsetCastCodecInfo(
				DataType.INT8, SCALE, 1.5, -64.0, 64.0, Rounding.TOWARDS_ZERO);

		final JsonElement json = gson.toJsonTree(original, CodecInfo.class);
		final CodecInfo restored = gson.fromJson(json, CodecInfo.class);

		assertEquals(original, restored);
	}

	@Test
	public void serializationRoundTripsWithDefaults() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
		final Gson gson = gsonBuilder.create();

		// only data_type set; scale/offset default, clamp bounds absent, rounding default
		final ScaleOffsetCastCodecInfo original = new ScaleOffsetCastCodecInfo(
				DataType.UINT16, ScaleOffsetCastCodecInfo.DEFAULT_SCALE, ScaleOffsetCastCodecInfo.DEFAULT_OFFSET,
				null, null, null);

		final JsonElement json = gson.toJsonTree(original, CodecInfo.class);
		final CodecInfo restored = gson.fromJson(json, CodecInfo.class);

		assertEquals(original, restored);
	}
}
