package org.janelia.saalfeldlab.n5.codec.reshape;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5Exception;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * The parsed {@code shape} configuration of a {@link ReshapeCodecInfo}.
 * <p>
 * Each output dimension is one of
 * <ul>
 * <li>a fixed positive size,</li>
 * <li>a group of input dimensions whose sizes are multiplied, or</li>
 * <li>{@link #INFER} ({@code -1}), inferred such that the number of elements is
 * preserved (at most once).</li>
 * </ul>
 * All dimension indices use N5 (F-order) conventions.
 * <p>
 * Instances are immutable.
 */
final class ReshapeShape {

	static final int INFER = -1;

	/**
	 * Per output dimension: a positive fixed size, {@link #INFER}, or
	 * {@code GROUP} if {@code groups[i]} is set.
	 */
	private static final int GROUP = 0;

	private final int[] sizes;

	private final int[][] groups;

	private ReshapeShape(final int[] sizes, final int[][] groups) {

		this.sizes = sizes;
		this.groups = groups;
	}

	static ReshapeShape parse(final JsonArray shape) {

		if (shape == null || shape.size() == 0)
			throw new N5Exception("Invalid reshape: shape must have at least one entry, got " + shape);

		final int m = shape.size();
		final int[] sizes = new int[m];
		final int[][] groups = new int[m][];
		int numInfer = 0;
		int lastInputDim = -1;
		for (int i = 0; i < m; ++i) {
			final JsonElement entry = shape.get(i);
			if (entry.isJsonArray()) {
				final JsonArray group = entry.getAsJsonArray();
				final int[] dims = new int[group.size()];
				for (int j = 0; j < dims.length; ++j) {
					dims[j] = asInt(group.get(j), shape);
					if (dims[j] <= lastInputDim)
						throw new N5Exception("Invalid reshape shape " + shape
								+ ": input dimensions must be strictly increasing over all entries");
					lastInputDim = dims[j];
				}
				sizes[i] = GROUP;
				groups[i] = dims;
			} else {
				final int size = asInt(entry, shape);
				if (size == INFER) {
					if (++numInfer > 1)
						throw new N5Exception("Invalid reshape shape " + shape + ": -1 may occur at most once");
				} else if (size <= 0)
					throw new N5Exception("Invalid reshape shape " + shape + ": sizes must be positive or -1");
				sizes[i] = size;
			}
		}
		return new ReshapeShape(sizes, groups);
	}

	private static int asInt(final JsonElement element, final JsonArray shape) {

		if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
			final double value = element.getAsDouble();
			if (value == Math.rint(value) && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
				return (int)value;
		}
		throw new N5Exception("Invalid reshape shape " + shape + ": expected an integer, got " + element);
	}

	JsonArray toJson() {

		final JsonArray json = new JsonArray(sizes.length);
		for (int i = 0; i < sizes.length; ++i) {
			if (groups[i] != null) {
				final JsonArray group = new JsonArray(groups[i].length);
				for (final int d : groups[i])
					group.add(new JsonPrimitive(d));
				json.add(group);
			} else
				json.add(new JsonPrimitive(sizes[i]));
		}
		return json;
	}

	int numOutputDimensions() {

		return sizes.length;
	}

	/**
	 * Checks that all input dimension indices are valid for inputs with
	 * {@code numInputDimensions} dimensions.
	 */
	void validate(final int numInputDimensions) {

		for (final int[] group : groups)
			if (group != null)
				for (final int d : group)
					if (d < 0 || d >= numInputDimensions)
						throw new N5Exception("Invalid reshape shape " + this + ": input dimension " + d
								+ " out of range for " + numInputDimensions + "-dimensional blocks");
	}

	/**
	 * Computes the output shape for an input of shape {@code a}, checking all
	 * constraints of the reshape specification.
	 *
	 * @param a
	 *            the input shape
	 * @return the output shape (a new array)
	 * @throws N5Exception
	 *             if {@code a} cannot be reshaped
	 */
	int[] outputShape(final int[] a) {

		validate(a.length);

		final int m = sizes.length;
		final long[] b = new long[m];
		int inferIndex = -1;
		long known = 1;
		for (int i = 0; i < m; ++i) {
			if (groups[i] != null) {
				b[i] = 1;
				for (final int d : groups[i])
					b[i] *= a[d];
			} else if (sizes[i] == INFER) {
				inferIndex = i;
				continue;
			} else
				b[i] = sizes[i];
			known *= b[i];
		}

		final long total = product(a, 0, a.length);
		if (inferIndex >= 0) {
			if (known == 0 || total % known != 0)
				throw error(a, "cannot infer the size of output dimension " + inferIndex);
			b[inferIndex] = total / known;
		} else if (known != total)
			throw error(a, "number of elements differs (" + known + " vs " + total + ")");

		final int[] result = new int[m];
		for (int i = 0; i < m; ++i) {
			if (b[i] > Integer.MAX_VALUE)
				throw error(a, "output dimension " + i + " is too large (" + b[i] + ")");
			result[i] = (int)b[i];
		}

		// grouped input dimensions must coincide with the raveled index along their output dimension
		for (int i = 0; i < m; ++i) {
			final int[] group = groups[i];
			if (group == null || group.length == 0)
				continue;
			final int first = group[0];
			final int last = group[group.length - 1];
			if (product(result, 0, i) != product(a, 0, first) || product(result, i + 1, m) != product(a, last + 1, a.length))
				throw error(a, "input dimensions " + Arrays.toString(group)
						+ " do not correspond to output dimension " + i);
		}

		return result;
	}

	private N5Exception error(final int[] a, final String reason) {

		return new N5Exception("Cannot reshape block of size " + Arrays.toString(a) + " with shape " + this + ": " + reason);
	}

	private static long product(final int[] x, final int from, final int to) {

		long p = 1;
		for (int i = from; i < to; ++i)
			p *= x[i];
		return p;
	}

	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof ReshapeShape))
			return false;
		final ReshapeShape other = (ReshapeShape)obj;
		return Arrays.equals(sizes, other.sizes) && Arrays.deepEquals(groups, other.groups);
	}

	@Override
	public int hashCode() {

		return 31 * Arrays.hashCode(sizes) + Arrays.deepHashCode(groups);
	}

	@Override
	public String toString() {

		return toJson().toString();
	}
}
