package org.janelia.saalfeldlab.n5.codec.reshape;

import java.util.function.UnaryOperator;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.SourceBlockSize;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

/**
 * Describes a reshape of blocks that preserves the order of their elements.
 * <p>
 * This is the N5 analog of the
 * <a href="https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/reshape/README.md">Zarr
 * reshape codec</a>, with all dimension indices in N5 (F-order) convention.
 * <p>
 * The {@code shape} parameter has one entry per output dimension, each one of
 * <ul>
 * <li>a positive integer: the fixed size of that output dimension,</li>
 * <li>an array of input dimension indices: the output size is the product of
 * their sizes, or</li>
 * <li>{@code -1}: the size is inferred such that the number of elements is
 * preserved (at most once).</li>
 * </ul>
 * Input dimension indices must be strictly increasing over all entries, and
 * grouped input dimensions must correspond to the raveled index along their
 * output dimension.
 * <p>
 * Example: {@code [3, [1], [2, 3]]} reshapes a block of size
 * {@code [3, 64, 50, 100]} to {@code [3, 64, 5000]}.
 */
@NameConfig.Name(value = ReshapeCodecInfo.TYPE)
public class ReshapeCodecInfo implements DatasetCodecInfo {

	public static final String TYPE = "n5-reshape";

	@NameConfig.Parameter
	private JsonArray shape;

	private transient volatile ReshapeShape parsed;

	public ReshapeCodecInfo() {
		// for serialization
	}

	public ReshapeCodecInfo(final JsonArray shape) {

		this.shape = shape.deepCopy();
		parsed();
	}

	/**
	 * Creates a reshape from the given entries, each either an {@code Integer}
	 * (a fixed size, or -1) or an {@code int[]} (a group of input dimensions).
	 *
	 * @param entries
	 *            the shape entries
	 * @return the reshape codec info
	 */
	public static ReshapeCodecInfo of(final Object... entries) {

		final JsonArray json = new JsonArray(entries.length);
		for (final Object entry : entries) {
			if (entry instanceof Integer)
				json.add(new JsonPrimitive((Integer)entry));
			else if (entry instanceof int[]) {
				final JsonArray group = new JsonArray();
				for (final int d : (int[])entry)
					group.add(new JsonPrimitive(d));
				json.add(group);
			} else
				throw new N5Exception("Invalid reshape entry " + entry + ": expected Integer or int[]");
		}
		return new ReshapeCodecInfo(json);
	}

	@Override
	public String getType() {

		return TYPE;
	}

	/**
	 * @return a copy of the shape configuration
	 */
	public JsonArray getShape() {

		return shape.deepCopy();
	}

	private ReshapeShape parsed() {

		// shape may be set by reflection during deserialization, so parse lazily
		ReshapeShape result = parsed;
		if (result == null)
			parsed = result = ReshapeShape.parse(shape);
		return result;
	}

	public int[] encodedBlockSize(final int[] blockSize) {

		return parsed().outputShape(blockSize);
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

		return new ReshapeCodec<>(parsed(), sourceBlockSize);
	}

	@Override
	public boolean equals(final Object obj) {

		return obj instanceof ReshapeCodecInfo && parsed().equals(((ReshapeCodecInfo)obj).parsed());
	}

	@Override
	public int hashCode() {

		return parsed().hashCode();
	}

	@Override
	public String toString() {

		return TYPE + parsed();
	}
}
