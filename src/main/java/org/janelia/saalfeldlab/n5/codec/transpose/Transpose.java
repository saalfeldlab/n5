package org.janelia.saalfeldlab.n5.codec.transpose;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.util.MemCopy;

class Transpose<T> {

	// TODO: detect when 1-sized dimensions are permuted. This should allow to
	//  simplify (or completely avoid) copying under certain conditions.

	public static int[] encode(final int[] decodedPos, final int[] order) {
		final int[] encodedPos = new int[decodedPos.length];
		encode(decodedPos, order, encodedPos);
		return encodedPos;
	}

	public static int[] decode(final int[] encodedPos, final int[] order) {
		final int[] decodedPos = new int[encodedPos.length];
		decode(encodedPos, order, decodedPos);
		return decodedPos;
	}

	public static void encode(int[] decodedPos, int[] order, int[] encodedPos) {
		for (int d = 0; d < order.length; d++)
			encodedPos[d] = decodedPos[order[d]];
	}

	public static void decode(int[] encodedPos, int[] order, int[] decodedPos) {
		for (int d = 0; d < order.length; d++)
			decodedPos[order[d]] = encodedPos[d];
	}

	@SuppressWarnings("unchecked")
	public static <T> Transpose<T> of(final DataType dataType, final int numDimensions) {
		return (Transpose<T>) new Transpose<>(MemCopy.forDataType(dataType), numDimensions);
	}

	private final MemCopy<T> memCopy;

	private final int[] ssize;
	private final int[] tsize;
	private final int[] ssteps;
	private final int[] tsteps;
	private final int[] csteps;

	Transpose(final MemCopy<T> memCopy, final int n) {
		this.memCopy = memCopy;
		ssize = new int[n];
		tsize = new int[n];
		ssteps = new int[n];
		tsteps = new int[n];
		csteps = new int[n];
	}

	public void encode(final T decoded, final T encoded, final int[] decodedSize, final int[] order) {
		final int n = ssize.length;

		for (int d = 0; d < n; ++d)
			ssize[d] = decodedSize[d];

		for (int d = 0; d < n; ++d)
			tsize[d] = decodedSize[order[d]];

		ssteps[0] = 1;
		for (int d = 0; d < n - 1; ++d)
			ssteps[d + 1] = ssteps[d] * ssize[d];

		tsteps[0] = 1;
		for (int d = 0; d < n - 1; ++d)
			tsteps[d + 1] = tsteps[d] * tsize[d];

		for (int d = 0; d < n; ++d)
			csteps[order[d]] = tsteps[d];

		copyRecursively(decoded, 0, encoded, 0, n - 1);
	}

	public void decode(final T encoded, final T decoded, final int[] decodedSize, final int[] order) {
		final int n = ssize.length;

		for (int d = 0; d < n; ++d)
			ssize[d] = decodedSize[order[d]];

		ssteps[0] = 1;
		for (int d = 0; d < n - 1; ++d)
			ssteps[d + 1] = ssteps[d] * ssize[d];

		tsteps[0] = 1;
		for (int d = 0; d < n - 1; ++d)
			tsteps[d + 1] = tsteps[d] * decodedSize[d];

		for (int d = 0; d < n; ++d)
			csteps[d] = tsteps[order[d]];

		copyRecursively(encoded, 0, decoded, 0, n - 1);
	}

	private void copyRecursively(final T src, final int srcPos, final T dest, final int destPos, final int d) {
		if (d == 0) {
			final int length = ssize[d];
			final int stride = csteps[d];
			memCopy.copyStrided(src, srcPos, dest, destPos, stride, length);
		} else {
			final int length = ssize[d];
			final int srcStride = ssteps[d];
			final int destStride = csteps[d];
			for (int i = 0; i < length; ++i)
				copyRecursively(src, srcPos + i * srcStride, dest, destPos + i * destStride, d - 1);
		}
	}

	Transpose<T> newInstance() {
		return new Transpose<>(memCopy, ssize.length);
	}

}
