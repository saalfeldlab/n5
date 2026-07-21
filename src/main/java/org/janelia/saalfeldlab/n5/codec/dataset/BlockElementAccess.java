package org.janelia.saalfeldlab.n5.codec.dataset;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * Element-wise access to the primitive arrays backing {@link org.janelia.saalfeldlab.n5.DataBlock
 * DataBlock}s, as {@code double}s.
 * <p>
 * A {@link DoubleReader} / {@link DoubleWriter} pair is obtained once per
 * {@link DataType}, so the type dispatch happens outside the element loop.
 * <p>
 * Note that {@code int64} and {@code uint64} values whose magnitude exceeds
 * 2<sup>53</sup> cannot be represented exactly as a {@code double} and are
 * therefore subject to precision loss.
 */
class BlockElementAccess {

	private BlockElementAccess() {}

	@FunctionalInterface
	interface DoubleReader {

		double get(Object array, int index);
	}

	@FunctionalInterface
	interface DoubleWriter {

		void set(Object array, int index, double value);
	}

	/**
	 * Returns a reader that interprets elements of an array of the given
	 * {@code dataType} as {@code double}s, respecting signedness.
	 */
	static DoubleReader reader(final DataType dataType) {

		switch (dataType) {
		case INT8:
			return (a, i) -> ((byte[])a)[i];
		case UINT8:
			return (a, i) -> ((byte[])a)[i] & 0xff;
		case INT16:
			return (a, i) -> ((short[])a)[i];
		case UINT16:
			return (a, i) -> ((short[])a)[i] & 0xffff;
		case INT32:
			return (a, i) -> ((int[])a)[i];
		case UINT32:
			return (a, i) -> ((int[])a)[i] & 0xffffffffL;
		case INT64:
			return (a, i) -> ((long[])a)[i];
		case UINT64:
			return (a, i) -> unsignedLongToDouble(((long[])a)[i]);
		case FLOAT32:
			return (a, i) -> ((float[])a)[i];
		case FLOAT64:
			return (a, i) -> ((double[])a)[i];
		default:
			throw new N5IOException("No numerical element access for data type " + dataType);
		}
	}

	/**
	 * Returns a writer that stores a {@code double} into an array of the given
	 * {@code dataType}.
	 * <p>
	 * Integral values are truncated to the width of the target type, so the
	 * caller is responsible for having brought {@code value} into range (by
	 * rounding, clamping, or wrapping) beforehand.
	 */
	static DoubleWriter writer(final DataType dataType) {

		switch (dataType) {
		case INT8:
		case UINT8:
			return (a, i, v) -> ((byte[])a)[i] = (byte)(long)v;
		case INT16:
		case UINT16:
			return (a, i, v) -> ((short[])a)[i] = (short)(long)v;
		case INT32:
		case UINT32:
			return (a, i, v) -> ((int[])a)[i] = (int)(long)v;
		case INT64:
			return (a, i, v) -> ((long[])a)[i] = (long)v;
		case UINT64:
			return (a, i, v) -> ((long[])a)[i] = doubleToUnsignedLong(v);
		case FLOAT32:
			return (a, i, v) -> ((float[])a)[i] = (float)v;
		case FLOAT64:
			return (a, i, v) -> ((double[])a)[i] = v;
		default:
			throw new N5IOException("No numerical element access for data type " + dataType);
		}
	}

	/**
	 * Interprets {@code value} as an unsigned 64-bit integer and returns it as a
	 * {@code double}.
	 */
	static double unsignedLongToDouble(final long value) {

		if (value >= 0)
			return value;

		// clear the sign bit, then add its weight back in
		return (value & Long.MAX_VALUE) + 0x1.0p63;
	}

	/**
	 * Returns the unsigned 64-bit integer nearest to {@code value}, as the raw
	 * bits of a {@code long}. Assumes {@code value} lies in
	 * {@code [0, 2}<sup>{@code 64}</sup>{@code )}.
	 */
	static long doubleToUnsignedLong(final double value) {

		if (value < 0x1.0p63)
			return (long)value;

		return (long)(value - 0x1.0p63) | Long.MIN_VALUE;
	}

	static boolean isFloatingPoint(final DataType dataType) {

		return dataType == DataType.FLOAT32 || dataType == DataType.FLOAT64;
	}

	/**
	 * Returns the number of bits of an integral {@code dataType}.
	 */
	static int bits(final DataType dataType) {

		switch (dataType) {
		case INT8:
		case UINT8:
			return 8;
		case INT16:
		case UINT16:
			return 16;
		case INT32:
		case UINT32:
			return 32;
		case INT64:
		case UINT64:
			return 64;
		default:
			throw new N5IOException(dataType + " is not an integral data type");
		}
	}

	/**
	 * Returns the smallest value representable by an integral {@code dataType}.
	 */
	static double minValue(final DataType dataType) {

		switch (dataType) {
		case INT8:
			return Byte.MIN_VALUE;
		case INT16:
			return Short.MIN_VALUE;
		case INT32:
			return Integer.MIN_VALUE;
		case INT64:
			return -0x1.0p63; // exactly Long.MIN_VALUE
		case UINT8:
		case UINT16:
		case UINT32:
		case UINT64:
			return 0;
		default:
			throw new N5IOException(dataType + " is not an integral data type");
		}
	}

	/**
	 * Returns the largest value representable by an integral {@code dataType}.
	 * <p>
	 * For {@code int64} and {@code uint64} the exact maximum is not
	 * representable as a {@code double}, so the largest {@code double} strictly
	 * below 2<sup>63</sup> (resp. 2<sup>64</sup>) is returned instead.
	 */
	static double maxValue(final DataType dataType) {

		switch (dataType) {
		case INT8:
			return Byte.MAX_VALUE;
		case UINT8:
			return 0xffL;
		case INT16:
			return Short.MAX_VALUE;
		case UINT16:
			return 0xffffL;
		case INT32:
			return Integer.MAX_VALUE;
		case UINT32:
			return 0xffffffffL;
		case INT64:
			return Math.nextDown(0x1.0p63);
		case UINT64:
			return Math.nextDown(0x1.0p64);
		default:
			throw new N5IOException(dataType + " is not an integral data type");
		}
	}
}
