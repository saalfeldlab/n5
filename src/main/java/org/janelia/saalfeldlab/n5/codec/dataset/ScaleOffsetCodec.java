package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.function.DoubleUnaryOperator;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;

/**
 * A same-type "array -&gt; array" codec that subtracts an {@code offset}, then
 * multiplies by a {@code scale} factor, computing element-wise in {@code double}
 * arithmetic.
 * <p>
 * Encoding: {@code out = (in - offset) * scale}
 * <br>
 * Decoding: {@code out = (in / scale) + offset}
 *
 * @param <T>
 *            the block data type
 *
 * @see ScaleOffsetCodecInfo
 */
public class ScaleOffsetCodec<T> implements DatasetCodec<T, T> {

	private final DataType dataType;

	private final double scale;

	private final double offset;

	public ScaleOffsetCodec(final DataType dataType, final double scale, final double offset) {

		this.dataType = dataType;
		this.scale = scale;
		this.offset = offset;
	}

	@Override
	public DataBlock<T> encode(final DataBlock<T> block) throws N5IOException {

		return apply(block, x -> (x - offset) * scale);
	}

	@Override
	public DataBlock<T> decode(final DataBlock<T> block) throws N5IOException {

		return apply(block, x -> (x / scale) + offset);
	}

	/**
	 * Applies {@code op} element-wise, reading each element as a {@code double}
	 * and writing the result back into a new block of the same {@link DataType}.
	 */
	@SuppressWarnings("unchecked")
	private DataBlock<T> apply(final DataBlock<T> block, final DoubleUnaryOperator op) throws N5IOException {

		final DataBlock<T> out = (DataBlock<T>)dataType.createDataBlock(
				block.getSize(), block.getGridPosition(), block.getNumElements());

		final Object src = block.getData();
		final Object dst = out.getData();

		switch (dataType) {
		case INT8: {
			final byte[] s = (byte[])src, d = (byte[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (byte)op.applyAsDouble(s[i]);
			break;
		}
		case UINT8: {
			final byte[] s = (byte[])src, d = (byte[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (byte)op.applyAsDouble(s[i] & 0xff);
			break;
		}
		case INT16: {
			final short[] s = (short[])src, d = (short[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (short)op.applyAsDouble(s[i]);
			break;
		}
		case UINT16: {
			final short[] s = (short[])src, d = (short[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (short)op.applyAsDouble(s[i] & 0xffff);
			break;
		}
		case INT32: {
			final int[] s = (int[])src, d = (int[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (int)op.applyAsDouble(s[i]);
			break;
		}
		case UINT32: {
			final int[] s = (int[])src, d = (int[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (int)(long)op.applyAsDouble(s[i] & 0xffffffffL);
			break;
		}
		case INT64: {
			final long[] s = (long[])src, d = (long[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (long)op.applyAsDouble(s[i]);
			break;
		}
		case UINT64: {
			final long[] s = (long[])src, d = (long[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (long)op.applyAsDouble(unsignedToDouble(s[i]));
			break;
		}
		case FLOAT32: {
			final float[] s = (float[])src, d = (float[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = (float)op.applyAsDouble(s[i]);
			break;
		}
		case FLOAT64: {
			final double[] s = (double[])src, d = (double[])dst;
			for (int i = 0; i < s.length; i++)
				d[i] = op.applyAsDouble(s[i]);
			break;
		}
		default:
			throw new N5IOException("ScaleOffsetCodec does not support data type " + dataType);
		}

		return out;
	}

	/**
	 * Interprets {@code value} as an unsigned 64-bit integer and returns it as a
	 * {@code double}.
	 */
	private static double unsignedToDouble(final long value) {

		double d = value & Long.MAX_VALUE;
		if (value < 0)
			d += 0x1.0p63; // add 2^63 back in for the sign bit
		return d;
	}

}
