package org.janelia.saalfeldlab.n5;

/**
 * Interface for data blocks. A data block has data, a position on the block
 * grid, and a size.
 *
 * @param <T> type of the data contained in the DataBlock
 *
 * @author Stephan Saalfeld
 */
public interface DataBlock<T> {

	/**
	 * Returns the size of this data block.
	 * <p>
	 * The size of a data block is expected to be smaller than or equal to the
	 * spacing of the block grid. The dimensionality of size is expected to be
	 * equal to the dimensionality of the dataset. Consistency is not enforced.
	 *
	 * @return size of the data block
	 */
	int[] getSize();

	/**
	 * Returns the position of this data block on the block grid relative to dataset.
	 * <p>
	 * The dimensionality of the grid position is expected to be equal to the
	 * dimensionality of the dataset. Consistency is not enforced.
	 *
	 * @return position on the block grid
	 */
	long[] getGridPosition();

	/**
	 * Returns the data object held by this data block.
	 *
	 * @return data object
	 */
	T getData();

	/**
	 * Returns the number of elements in this {@link DataBlock}. This number is
	 * not necessarily equal {@link #getNumElements(int[])
	 * getNumElements(getSize())}.
	 *
	 * @return the number of elements
	 */
	int getNumElements();

	/**
	 * Returns the number of elements in a box of given size.
	 *
	 * @param size
	 *            the size
	 * @return the number of elements
	 */
	static int getNumElements(final int[] size) {

		int n = size[0];
		for (int i = 1; i < size.length; ++i)
			n *= size[i];
		return n;
	}

	/**
	 * Factory for creating {@code DataBlock<T>}.
	 *
	 * @param <T>
	 * 		type of the data contained in the DataBlock
	 */
	interface DataBlockFactory<T> {

		/**
		 * Create a new {@link DataBlock} with the given {@code blockSize}, {@code gridPosition}, and {@code data} content.
		 *
		 * @param blockSize
		 * 		the block size
		 * @param gridPosition
		 * 		the grid position
		 * @param data
		 * 		the data object
		 *
		 * @return a new DataBlock
		 */
		DataBlock<T> createDataBlock(int[] blockSize, long[] gridPosition, T data);
	}
}
