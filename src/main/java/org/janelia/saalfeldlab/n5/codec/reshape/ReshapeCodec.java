package org.janelia.saalfeldlab.n5.codec.reshape;

import java.util.Arrays;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;

/**
 * Reshapes {@link DataBlock}s between two fixed shapes without reordering
 * their elements.
 * <p>
 * Encoding gives every block the {@code encodedShape}, decoding gives it the
 * {@code decodedShape}. Both must have the same number of elements, and so must
 * every block passed to the codec. This codec checks the number of
 * data block elements when encoding / decoding, not an the exact
 * data block shape.
 * <p>
 * Instances are immutable and thread-safe.
 *
 * @param <T>
 *            the type of the block data
 */
public class ReshapeCodec<T> implements DatasetCodec<T, T> {

	private final int[] decodedShape;

	private final int[] encodedShape;

	private final int numElements;

	/**
	 * @param decodedShape
	 *            the shape of decoded blocks
	 * @param encodedShape
	 *            the shape of encoded blocks
	 */
	public ReshapeCodec(final int[] decodedShape, final int[] encodedShape) {

		this.numElements = DataBlock.getNumElements(decodedShape);
		if (DataBlock.getNumElements(encodedShape) != numElements)
			throw new N5Exception("Cannot reshape " + Arrays.toString(decodedShape) + " to " + Arrays.toString(encodedShape)
					+ ": the number of elements differs");

		this.decodedShape = decodedShape.clone();
		this.encodedShape = encodedShape.clone();
	}

	public int[] getDecodedShape() {

		return decodedShape.clone();
	}

	public int[] getEncodedShape() {

		return encodedShape.clone();
	}

	@Override
	public DataBlock<T> encode(final DataBlock<T> dataBlock) {

		return reshape(dataBlock, encodedShape);
	}

	@Override
	public DataBlock<T> decode(final DataBlock<T> dataBlock) {

		return reshape(dataBlock, decodedShape);
	}
	
	private DataBlock<T> reshape(final DataBlock<T> dataBlock, int[] newShape) {

		checkNumElements(dataBlock, newShape);
		return wrap(newShape.clone(), dataBlock.getGridPosition(), dataBlock.getData());
	}

	private void checkNumElements(final DataBlock<T> dataBlock, final int[] expectedShape) {

		if (dataBlock.getNumElements() != numElements)
			throw new N5Exception("Cannot reshape block at " + Arrays.toString(dataBlock.getGridPosition())
					+ " of size " + Arrays.toString(dataBlock.getSize()) + " (" + dataBlock.getNumElements()
					+ " elements): reshape expects blocks of size " + Arrays.toString(expectedShape)
					+ " (" + numElements + " elements)");
	}

	@SuppressWarnings("unchecked")
	private static <T> DataBlock<T> wrap(final int[] size, final long[] gridPosition, final T data) {

		final DataBlock<?> block;
		if (data instanceof byte[])
			block = new ByteArrayDataBlock(size, gridPosition, (byte[])data);
		else if (data instanceof short[])
			block = new ShortArrayDataBlock(size, gridPosition, (short[])data);
		else if (data instanceof int[])
			block = new IntArrayDataBlock(size, gridPosition, (int[])data);
		else if (data instanceof long[])
			block = new LongArrayDataBlock(size, gridPosition, (long[])data);
		else if (data instanceof float[])
			block = new FloatArrayDataBlock(size, gridPosition, (float[])data);
		else if (data instanceof double[])
			block = new DoubleArrayDataBlock(size, gridPosition, (double[])data);
		else if (data instanceof String[])
			block = new StringDataBlock(size, gridPosition, (String[])data);
		else
			throw new N5Exception("Reshape does not support blocks with data of type "
					+ (data == null ? "null" : data.getClass().getSimpleName()));
		return (DataBlock<T>)block;
	}
}
