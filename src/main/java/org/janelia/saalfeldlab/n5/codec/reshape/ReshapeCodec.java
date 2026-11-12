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
import org.janelia.saalfeldlab.n5.codec.SourceBlockSize;

/**
 * Reshapes {@link DataBlock}s without reordering their elements.
 * <p>
 * Since N5 blocks are stored in F-order, and a reshape preserves the
 * (F-order) raveled element order, only the block size changes; the data array
 * is shared, never copied.
 * <p>
 * Decoding needs the input shape, which cannot in general be recovered from
 * the output shape (merged dimensions cannot be split). It is taken from the
 * {@link SourceBlockSize}: the full source size if the block has as many
 * elements as a full chunk, otherwise the cropped size at the block's grid
 * position.
 * <p>
 * Instances are immutable and thread-safe.
 *
 * @param <T>
 *            the type of the block data
 */
public class ReshapeCodec<T> implements DatasetCodec<T, T> {

	private final ReshapeShape shape;

	private final SourceBlockSize sourceBlockSize;

	private final int[] fullSourceSize;

	private final int fullNumElements;

	ReshapeCodec(final ReshapeShape shape, final SourceBlockSize sourceBlockSize) {

		this.shape = shape;
		this.sourceBlockSize = sourceBlockSize;
		this.fullSourceSize = sourceBlockSize.full();
		this.fullNumElements = DataBlock.getNumElements(fullSourceSize);

		// fail early on configurations that cannot reshape a full chunk
		shape.outputShape(fullSourceSize);
	}

	@Override
	public DataBlock<T> encode(final DataBlock<T> dataBlock) {

		final int[] encodedSize = shape.outputShape(dataBlock.getSize());
		return wrap(encodedSize, dataBlock.getGridPosition(), dataBlock.getData());
	}

	@Override
	public DataBlock<T> decode(final DataBlock<T> dataBlock) {

		final int numElements = dataBlock.getNumElements();
		final int[] decodedSize = numElements == fullNumElements
				? fullSourceSize.clone()
				: sourceBlockSize.at(dataBlock.getGridPosition());

		if (DataBlock.getNumElements(decodedSize) != numElements)
			throw new N5Exception("Cannot decode reshaped block at " + Arrays.toString(dataBlock.getGridPosition())
					+ ": it has " + numElements + " elements, expected a block of size " + Arrays.toString(decodedSize));

		return wrap(decodedSize, dataBlock.getGridPosition(), dataBlock.getData());
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
