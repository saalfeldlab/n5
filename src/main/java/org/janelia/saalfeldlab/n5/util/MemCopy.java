package org.janelia.saalfeldlab.n5.util;

import org.janelia.saalfeldlab.n5.DataType;

/**
 * Low-level range copying methods between source and target primitve array
 * (type {@code T}, e.g., {@code double[]}).
 *
 * @param <T>
 * 		the source/target type. Must be a primitive array type (e.g., {@code double[]})
 */
public interface MemCopy<T> {

	MemCopyByte BYTE = new MemCopyByte();
	MemCopyShort SHORT = new MemCopyShort();
	MemCopyInt INT = new MemCopyInt();
	MemCopyLong LONG = new MemCopyLong();
	MemCopyFloat FLOAT = new MemCopyFloat();
	MemCopyDouble DOUBLE = new MemCopyDouble();

	static MemCopy<?> forDataType(final DataType dataType) {
		switch (dataType) {
		case UINT8:
		case INT8:
			return BYTE;
		case UINT16:
		case INT16:
			return SHORT;
		case UINT32:
		case INT32:
			return INT;
		case UINT64:
		case INT64:
			return LONG;
		case FLOAT32:
			return FLOAT;
		case FLOAT64:
			return DOUBLE;
		case STRING:
		case OBJECT:
			throw new UnsupportedOperationException("TODO?");
		default:
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Copy {@code length} components from the {@code src} array to the {@code
	 * dest} array. The components at positions {@code srcPos} through {@code
	 * srcPos+length-1} in the source array are copied into positions {@code
	 * destPos}, {@code destPos+destStride}, {@code destPos + 2*destStride},
	 * etc., through {@code destPos+(length-1)*destStride} of the destination
	 * array.
	 */
	void copyStrided(T src, int srcPos, T dest, int destPos, int destStride, int length);

	class MemCopyByte implements MemCopy<byte[]> {

		@Override
		public void copyStrided(final byte[] src, final int srcPos, final byte[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}

	class MemCopyShort implements MemCopy<short[]> {

		@Override
		public void copyStrided(final short[] src, final int srcPos, final short[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}

	class MemCopyInt implements MemCopy<int[]> {

		@Override
		public void copyStrided(final int[] src, final int srcPos, final int[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}

	class MemCopyLong implements MemCopy<long[]> {

		@Override
		public void copyStrided(final long[] src, final int srcPos, final long[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}

	class MemCopyFloat implements MemCopy<float[]> {

		@Override
		public void copyStrided(final float[] src, final int srcPos, final float[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}

	class MemCopyDouble implements MemCopy<double[]> {

		@Override
		public void copyStrided(final double[] src, final int srcPos, final double[] dest, final int destPos, final int destStride, final int length) {
			if (destStride == 1)
				System.arraycopy(src, srcPos, dest, destPos, length);
			else
				for (int i = 0; i < length; ++i)
					dest[destPos + i * destStride] = src[srcPos + i];
		}
	}
}
