package org.janelia.saalfeldlab.n5.codec.reshape;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.SourceBlockSize;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Describes a reshape of blocks that preserves the order of their elements.
 * <p>
 * This is the N5 analog of the
 * <a href="https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/reshape/README.md">Zarr
 * reshape codec</a>, with all dimension indices in N5 (F-order) convention.
 * <p>
 * The {@code shape} parameter has one {@link ReshapeEntry} per output
 * dimension, each one of
 * <ul>
 * <li>{@link ReshapeEntry#size(int)}: the fixed size of that output
 * dimension,</li>
 * <li>{@link ReshapeEntry#dims(int...)}: the output size is the product of the
 * sizes of these input dimensions, or</li>
 * <li>{@link ReshapeEntry#infer()}: the size is inferred such that the number
 * of elements is preserved (at most once).</li>
 * </ul>
 * Input dimension indices must be strictly increasing over all entries, and
 * grouped input dimensions must correspond to the raveled index along their
 * output dimension.
 * <p>
 * Example: with {@code import static ...ReshapeEntry.*},
 * {@code ReshapeCodecInfo.of(size(3), dims(1), dims(2, 3))} (JSON
 * {@code [3, [1], [2, 3]]}) reshapes a block of size {@code [3, 64, 50, 100]} to
 * {@code [3, 64, 5000]}.
 * <p>
 * This class validates the {@code shape} configuration and resolves it against
 * the size of the blocks the codec receives (see
 * {@link #encodedBlockSize(int[])}). The {@link ReshapeCodec} it creates only
 * maps between the two resulting fixed shapes.
 */
@NameConfig.Name(value = ReshapeCodecInfo.TYPE)
public class ReshapeCodecInfo implements DatasetCodecInfo {

	public static final String TYPE = "n5-reshape";

	@NameConfig.Parameter
	private ReshapeEntry[] shape;

	public ReshapeCodecInfo() {
		// for serialization
	}

	/**
	 * @param shape
	 *            the reshape configuration, one entry per output dimension
	 * @throws N5Exception
	 *             if the configuration is invalid
	 */
	public ReshapeCodecInfo(final ReshapeEntry[] shape) {

		this.shape = shape.clone();
		ReshapeEntry.validate(this.shape);
	}

	/**
	 * Creates a reshape from the given entries, one per output dimension.
	 *
	 * @param shape
	 *            the reshape configuration
	 * @return the reshape codec info
	 * @throws N5Exception
	 *             if the configuration is invalid
	 */
	public static ReshapeCodecInfo of(final ReshapeEntry... shape) {

		return new ReshapeCodecInfo(shape);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	/**
	 * @return the shape configuration (a copy of the array; entries are immutable)
	 */
	public ReshapeEntry[] getShape() {

		return shape.clone();
	}

	/**
	 * Computes the shape of encoded blocks for source blocks of the given size,
	 * checking all constraints of the reshape specification.
	 *
	 * @param blockSize
	 *            the size of the source block
	 * @return the encoded block size (a new array)
	 * @throws N5Exception
	 *             if blocks of size {@code blockSize} cannot be reshaped
	 */
	@Override
	public int[] encodedBlockSize(final int[] blockSize) {

		validate(blockSize);
		final long[] b = outputShape(blockSize);
		final int[] result = new int[b.length];
		for (int i = 0; i < b.length; ++i)
			result[i] = (int)b[i];
		return result;
	}

	@Override
	public ReshapeCodec<?> create(final DatasetAttributes attributes) {

		return create(attributes, attributes.getDataType());
	}

	@Override
	public ReshapeCodec<?> create(final DatasetAttributes attributes, final DataType sourceDataType) {

		// assumes this codec is first in the chain (or preceded only by shape-preserving codecs)
		return create(attributes, sourceDataType,
				SourceBlockSize.of(attributes.getDimensions(), attributes.getChunkSize(), UnaryOperator.identity()));
	}

	@Override
	public ReshapeCodec<?> create(final DatasetAttributes attributes, final DataType sourceDataType,
			final SourceBlockSize sourceBlockSize) {

		final int[] decodedShape = sourceBlockSize.full();
		return new ReshapeCodec<>(decodedShape, encodedBlockSize(decodedShape));
	}

	/**
	 * Checks that blocks of size {@code a} can be reshaped with this
	 * configuration: all constraints of the reshape specification, including
	 * those that do not depend on {@code a} (checked here too, since
	 * deserialization sets {@code shape} by reflection, bypassing the
	 * constructor).
	 *
	 * @param a
	 *            the size of the source block
	 * @throws N5Exception
	 *             if blocks of size {@code a} cannot be reshaped
	 */
	private void validate(final int[] a) {

		ReshapeEntry.validate(shape);

		// input dimensions must exist (before computing the output shape, which indexes a with them)
		for (final ReshapeEntry entry : shape) {
			final int[] dims = entry.inputDims();
			if (dims != null)
				for (final int d : dims)
					if (d >= a.length)
						throw error(a, "input dimension " + d + " out of range");
		}

		final long[] b = outputShape(a);
		final int m = b.length;

		// the number of elements must be preserved
		final long total = product(a, 0, a.length);
		final long outputTotal = product(b, 0, m);
		if (outputTotal != total) {
			for (int i = 0; i < m; ++i)
				if (shape[i].isInferred())
					throw error(a, "cannot infer the size of output dimension " + i);
			throw error(a, "number of elements differs (" + outputTotal + " vs " + total + ")");
		}

		for (int i = 0; i < m; ++i)
			if (b[i] > Integer.MAX_VALUE)
				throw error(a, "output dimension " + i + " is too large (" + b[i] + ")");

		// grouped input dimensions must coincide with the raveled index along their output dimension
		for (int i = 0; i < m; ++i) {
			final int[] dims = shape[i].inputDims();
			if (dims == null || dims.length == 0)
				continue;
			final int first = dims[0];
			final int last = dims[dims.length - 1];
			if (product(b, 0, i) != product(a, 0, first) || product(b, i + 1, m) != product(a, last + 1, a.length))
				throw error(a, "input dimensions " + Arrays.toString(dims) + " do not correspond to output dimension " + i);
		}
	}

	/**
	 * Computes the output shape for blocks of size {@code a}, without checking
	 * any constraints (see {@link #validate(int[])}). An inferred size is
	 * rounded down if the number of elements is not divisible.
	 *
	 * @param a
	 *            the size of the source block, all input dimensions in range
	 * @return the output shape
	 */
	private long[] outputShape(final int[] a) {

		final int m = shape.length;
		final long[] b = new long[m];
		int inferIndex = -1;
		long known = 1;
		for (int i = 0; i < m; ++i) {
			final ReshapeEntry entry = shape[i];
			final int[] dims = entry.inputDims();
			if (dims != null) {
				b[i] = 1;
				for (final int d : dims)
					b[i] *= a[d];
			} else if (entry.isInferred()) {
				inferIndex = i;
				continue;
			} else
				b[i] = entry.fixedSize();

			known *= b[i];
		}

		if (inferIndex >= 0)
			b[inferIndex] = known == 0 ? 0 : product(a, 0, a.length) / known;

		return b;
	}

	private N5Exception error(final int[] a, final String reason) {

		return new N5Exception("Cannot reshape block of size " + Arrays.toString(a) + " with shape "
				+ Arrays.toString(shape) + ": " + reason);
	}

	private static long product(final int[] x, final int from, final int to) {

		long p = 1;
		for (int i = from; i < to; ++i)
			p *= x[i];
		return p;
	}

	private static long product(final long[] x, final int from, final int to) {

		long p = 1;
		for (int i = from; i < to; ++i)
			p *= x[i];
		return p;
	}

	@Override
	public boolean equals(final Object obj) {

		return obj instanceof ReshapeCodecInfo && Arrays.equals(shape, ((ReshapeCodecInfo)obj).shape);
	}

	@Override
	public int hashCode() {

		return Arrays.hashCode(shape);
	}

	@Override
	public String toString() {

		return TYPE + Arrays.toString(shape);
	}
}
