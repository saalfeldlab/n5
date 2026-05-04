package org.janelia.saalfeldlab.n5;

import java.util.function.ToIntFunction;

/**
 * Abstract base class for {@link DataBlock} implementations.
 *
 * @param <T>
 *            the block data type
 *
 * @author Stephan Saalfeld
 */
public abstract class AbstractDataBlock<T> implements DataBlock<T> {

	protected final int[] size;
	protected final long[] gridPosition;
	protected final T data;
	private final ToIntFunction<T> numElements;

	public AbstractDataBlock(
			final int[] size,
			final long[] gridPosition,
			final T data,
			final ToIntFunction<T> numElements) {

		this.size = size;
		this.gridPosition = gridPosition;
		this.data = data;
		this.numElements = numElements;
	}

	@Override
	public int[] getSize() {

		return size;
	}

	@Override
	public long[] getGridPosition() {

		return gridPosition;
	}

	@Override
	public T getData() {

		return data;
	}

	@Override
	public int getNumElements() {

		return numElements.applyAsInt(data);
	}
}
