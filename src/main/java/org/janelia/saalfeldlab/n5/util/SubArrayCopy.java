package org.janelia.saalfeldlab.n5.util;

/**
 * Copy sub-region between flattened arrays (of different sizes).
 * <p>
 * The {@link #copy(Object, int[], int[], Object, int[], int[], int[]) SubArrayCopy.copy}
 * method requires the nD size of the flattened source and target arrays, the nD
 * starting position and nD size of the source region to copy, and the nD
 * starting position in the target to copy to.
 */
public interface SubArrayCopy
{
	/**
	 * Copy a nD region from {@code src} to {@code dest}, where {@code src} and
	 * {@code dest} are flattened nD array of dimensions {@code srcSize} and
	 * {@code destSize}, respectively.
	 *
	 * @param src
	 * 		flattened nD source array
	 * @param srcSize
	 * 		dimensions of src
	 * @param srcPos
	 * 		starting position, in src, of the range to copy
	 * @param dest
	 * 		flattened nD destination array
	 * @param destSize
	 * 		dimensions of dest
	 * @param destPos
	 * 		starting position, in dest, of the range to copy
	 * @param size
	 * 		size of the range to copy
	 */
	// TODO: generic T instead of Object to make sure src and dest are the same primitive array type
	static void copy( Object src, int[] srcSize, int[] srcPos, Object dest, int[] destSize, int[] destPos, int[] size )
	{
		final int n = srcSize.length;
		assert srcPos.length == n;
		assert destSize.length == n;
		assert destPos.length == n;
		assert size.length == n;

		final int[] srcStrides = createAllocationSteps( srcSize );
		final int[] destStrides = createAllocationSteps( destSize );
		final int oSrc = positionToIndex( srcPos, srcSize );
		final int oDest = positionToIndex( destPos, destSize );

		copyNDRangeRecursive( n - 1,
				src, srcStrides, oSrc,
				dest, destStrides, oDest,
				size );
	}

	// TODO: maybe hide the implementation details below in an inner class

	static int positionToIndex( final int[] position, final int[] dimensions )
	{
		final int maxDim = dimensions.length - 1;
		int i = position[ maxDim ];
		for ( int d = maxDim - 1; d >= 0; --d )
			i = i * dimensions[ d ] + position[ d ];
		return i;
	}

	/**
	 * Create allocation step array from the dimensions of an N-dimensional
	 * array.
	 *
	 * @param dimensions
	 * @param steps
	 */
	// TODO: rename to something with "stride"
	// TODO: inline into method below (or always use this one)
	static void createAllocationSteps( final int[] dimensions, final int[] steps )
	{
		steps[ 0 ] = 1;
		for ( int d = 1; d < dimensions.length; ++d )
			steps[ d ] = steps[ d - 1 ] * dimensions[ d - 1 ];
	}

	// TODO: rename to something with "stride"
	// TODO: inline above (or always use that one)
	static int[] createAllocationSteps( final int[] dimensions )
	{
		final int[] steps = new int[ dimensions.length ];
		createAllocationSteps( dimensions, steps );
		return steps;
	}


	/**
	 * Recursively copy a {@code (d+1)} dimensional region from {@code src} to
	 * {@code dest}, where {@code src} and {@code dest} are flattened nD array
	 * with strides {@code srcStrides} and {@code destStrides}, respectively.
	 * <p>
	 * For {@code d=0}, a 1D line of length {@code size[0]} is copied
	 * (equivalent to {@code System.arraycopy}). For {@code d=1}, a 2D plane of
	 * size {@code size[0] * size[1]} is copied, by recursively copying 1D
	 * lines, starting {@code srcStrides[1]} (respectively {@code
	 * destStrides[1]}) apart. For {@code d=2}, a 3D box is copied by
	 * recursively copying 2D planes, etc.
	 *
	 * @param d
	 * 		current dimension
	 * @param src
	 * 		flattened nD source array
	 * @param srcStrides
	 *      nD strides of src
	 * @param srcPos
	 * 		flattened index (in src) to start copying from
	 * @param dest
	 * 		flattened nD destination array
	 * @param destStrides
	 *      nD strides of dest
	 * @param destPos
	 * 		flattened index (in dest) to start copying to
	 * @param size
	 * 		nD size of the range to copy
	 */
	static <T> void copyNDRangeRecursive(
			final int d,
			final T src,
			final int[] srcStrides,
			final int srcPos,
			final T dest,
			final int[] destStrides,
			final int destPos,
			final int[] size
	) {
		final int len = size[d];
		if (d > 0) {
			final int stride_src = srcStrides[d];
			final int stride_dst = destStrides[d];
			for (int i = 0; i < len; ++i)
				copyNDRangeRecursive(d - 1,
						src, srcStrides, srcPos + i * stride_src,
						dest, destStrides, destPos + i * stride_dst,
						size);
		} else
			System.arraycopy(src, srcPos, dest, destPos, len);
	}
}
