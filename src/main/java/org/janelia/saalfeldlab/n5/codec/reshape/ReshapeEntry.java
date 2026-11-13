package org.janelia.saalfeldlab.n5.codec.reshape;

import java.lang.reflect.Type;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5Exception;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;

/**
 * One entry of a reshape {@code shape} configuration, describing the size of
 * one output dimension. An entry is one of
 * <ul>
 * <li>{@link #size(int)}: a fixed, positive size,</li>
 * <li>{@link #dims(int...)}: the product of the sizes of the given input
 * dimensions, or</li>
 * <li>{@link #infer()}: the size is inferred such that the number of elements
 * is preserved.</li>
 * </ul>
 * For example, with {@code import static ...ReshapeEntry.*}:
 *
 * <pre>
 * ReshapeCodecInfo.of(size(3), dims(1), dims(2, 3)); // JSON: [3, [1], [2, 3]]
 * </pre>
 * <p>
 * Entries serialize to the JSON of the reshape specification: a fixed size as
 * an integer, input dimensions as an array of integers, and an inferred size as
 * {@code -1}.
 * <p>
 * Entries are immutable. Whether input dimension indices refer to N5 (F-order)
 * or Zarr (C-order) dimensions depends on the codec they are used with.
 */
@JsonAdapter(ReshapeEntry.Adapter.class)
public abstract class ReshapeEntry {

	/** The JSON value of an inferred entry. */
	public static final int INFER = -1;

	private ReshapeEntry() {
		// only the nested implementations
	}

	/**
	 * @param size
	 *            the size of the output dimension, positive
	 * @return an entry of fixed size
	 */
	public static ReshapeEntry size(final int size) {

		if (size <= 0)
			throw new N5Exception("Invalid reshape entry " + size + ": sizes must be positive");
		return new Size(size);
	}

	/**
	 * @param inputDims
	 *            the input dimensions merged into the output dimension, strictly
	 *            increasing
	 * @return an entry whose size is the product of the sizes of the
	 *         {@code inputDims}
	 */
	public static ReshapeEntry dims(final int... inputDims) {

		for (final int d : inputDims)
			if (d < 0)
				throw new N5Exception("Invalid reshape entry " + Arrays.toString(inputDims)
						+ ": input dimensions must not be negative");
		return new Dims(inputDims.clone());
	}

	/**
	 * @return an entry whose size is inferred
	 */
	public static ReshapeEntry infer() {

		return Infer.INSTANCE;
	}

	/**
	 * Checks the constraints of the reshape specification that do not depend on
	 * the shape of the input: the shape has at least one entry, at most one is
	 * {@link #infer()}, and the input dimensions are strictly increasing over all
	 * entries. These hold in either dimension order, see
	 * {@link #reverseDimensions(ReshapeEntry[], int)}.
	 *
	 * @param shape
	 *            the reshape configuration
	 * @throws N5Exception
	 *             if a constraint is violated
	 */
	public static void validate(final ReshapeEntry[] shape) {

		if (shape == null || shape.length == 0)
			throw new N5Exception("Invalid reshape: shape must have at least one entry");

		int numInfer = 0;
		int lastInputDim = -1;
		for (final ReshapeEntry entry : shape) {
			if (entry == null)
				throw new N5Exception("Invalid reshape shape " + Arrays.toString(shape) + ": null entry");
			if (entry.isInferred() && ++numInfer > 1)
				throw new N5Exception("Invalid reshape shape " + Arrays.toString(shape) + ": -1 may occur at most once");
			final int[] dims = entry.inputDims();
			if (dims != null)
				for (final int d : dims) {
					if (d <= lastInputDim)
						throw new N5Exception("Invalid reshape shape " + Arrays.toString(shape)
								+ ": input dimensions must be strictly increasing over all entries");
					lastInputDim = d;
				}
		}
	}

	/**
	 * Converts a reshape configuration between N5 (F-order) and Zarr (C-order)
	 * dimension conventions: reverses the entries and maps every input
	 * dimension {@code d} to {@code numInputDimensions - 1 - d}. The conversion
	 * is its own inverse.
	 * <p>
	 * Since a reshape preserves the raveled element order, and C-order raveling
	 * of a Zarr chunk is F-order raveling of the same chunk in N5, the result
	 * describes the same reshape in the other convention.
	 *
	 * @param shape
	 *            the reshape configuration
	 * @param numInputDimensions
	 *            the number of dimensions of the blocks to reshape
	 * @return the converted configuration (a new array)
	 * @throws N5Exception
	 *             if an input dimension is out of range
	 */
	public static ReshapeEntry[] reverseDimensions(final ReshapeEntry[] shape, final int numInputDimensions) {

		final int m = shape.length;
		final ReshapeEntry[] reversed = new ReshapeEntry[m];
		for (int i = 0; i < m; ++i) {
			final ReshapeEntry entry = shape[m - 1 - i];
			final int[] dims = entry == null ? null : entry.inputDims();
			if (dims == null)
				reversed[i] = entry;
			else {
				final int[] reversedDims = new int[dims.length];
				for (int j = 0; j < dims.length; ++j) {
					final int d = dims[dims.length - 1 - j];
					if (d >= numInputDimensions)
						throw new N5Exception("Invalid reshape shape " + Arrays.toString(shape) + ": input dimension "
								+ d + " out of range for " + numInputDimensions + " dimensions");
					reversedDims[j] = numInputDimensions - 1 - d;
				}
				reversed[i] = new Dims(reversedDims);
			}
		}
		return reversed;
	}

	/**
	 * @return the input dimensions of a {@link #dims(int...)} entry (not a
	 *         copy, do not modify), or {@code null}
	 */
	abstract int[] inputDims();

	/**
	 * @return the size of a {@link #size(int)} entry, or {@code 0}
	 */
	abstract int fixedSize();

	abstract boolean isInferred();

	abstract JsonElement toJson();

	static ReshapeEntry fromJson(final JsonElement json) {

		if (json.isJsonArray()) {
			final JsonArray array = json.getAsJsonArray();
			final int[] dims = new int[array.size()];
			for (int i = 0; i < dims.length; ++i)
				dims[i] = asInt(array.get(i), json);
			return dims(dims);
		}
		final int value = asInt(json, json);
		return value == INFER ? infer() : size(value);
	}

	private static int asInt(final JsonElement element, final JsonElement entry) {

		if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
			final double value = element.getAsDouble();
			if (value == Math.rint(value) && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
				return (int)value;
		}
		throw new N5Exception("Invalid reshape entry " + entry + ": expected an integer, got " + element);
	}

	/**
	 * (De)serializes entries to and from the JSON of the reshape specification.
	 * Registered by the {@link JsonAdapter} annotation, so needs no
	 * registration with a {@code GsonBuilder}.
	 */
	static final class Adapter implements JsonSerializer<ReshapeEntry>, JsonDeserializer<ReshapeEntry> {

		@Override
		public JsonElement serialize(final ReshapeEntry entry, final Type typeOfSrc, final JsonSerializationContext context) {

			return entry.toJson();
		}

		@Override
		public ReshapeEntry deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) {

			return fromJson(json);
		}
	}

	private static final class Size extends ReshapeEntry {

		private final int size;

		Size(final int size) {

			this.size = size;
		}

		@Override
		int[] inputDims() {

			return null;
		}

		@Override
		int fixedSize() {

			return size;
		}

		@Override
		boolean isInferred() {

			return false;
		}

		@Override
		JsonElement toJson() {

			return new JsonPrimitive(size);
		}

		@Override
		public boolean equals(final Object obj) {

			return obj instanceof Size && ((Size)obj).size == size;
		}

		@Override
		public int hashCode() {

			return size;
		}
	}

	private static final class Dims extends ReshapeEntry {

		private final int[] dims;

		Dims(final int[] dims) {

			this.dims = dims;
		}

		@Override
		int[] inputDims() {

			return dims;
		}

		@Override
		int fixedSize() {

			return 0;
		}

		@Override
		boolean isInferred() {

			return false;
		}

		@Override
		JsonElement toJson() {

			final JsonArray json = new JsonArray(dims.length);
			for (final int d : dims)
				json.add(d);
			return json;
		}

		@Override
		public boolean equals(final Object obj) {

			return obj instanceof Dims && Arrays.equals(((Dims)obj).dims, dims);
		}

		@Override
		public int hashCode() {

			return 31 * Arrays.hashCode(dims) + 1;
		}
	}

	private static final class Infer extends ReshapeEntry {

		static final Infer INSTANCE = new Infer();

		@Override
		int[] inputDims() {

			return null;
		}

		@Override
		int fixedSize() {

			return 0;
		}

		@Override
		boolean isInferred() {

			return true;
		}

		@Override
		JsonElement toJson() {

			return new JsonPrimitive(INFER);
		}
	}
}
