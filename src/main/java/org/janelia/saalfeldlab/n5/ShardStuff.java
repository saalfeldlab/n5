package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class ShardStuff {

	public interface Block {

		/**
		 * Returns the number of elements in a box of given size.
		 *
		 * @param size
		 * 		the size
		 *
		 * @return the number of elements
		 */
		static int getNumElements(final int[] size) {

			int n = size[0];
			for (int i = 1; i < size.length; ++i)
				n *= size[i];
			return n;
		}

		/**
		 * Returns the size of this block.
		 * <p>
		 * The size of a data block is expected to be smaller than or equal to the
		 * spacing of the block grid. The dimensionality of size is expected to be
		 * equal to the dimensionality of the dataset. Consistency is not enforced.
		 *
		 * @return size of the block
		 */
		int[] getSize();

		/**
		 * Returns the grid position of this block.
		 * <p>
		 * Grid position is with respect to the level of the block (Shard, nested Shard, DataBlock, ...).
		 * If a block has grid position (i, ...) the adjacent block (in X) has position (i+1, ...).
		 * Grid position (0, ...) is the origin of the image, that is, grid positions are not relative to the containing shard etc.
		 * <p>
		 * The dimensionality of the grid position is expected to be equal to the
		 * dimensionality of the dataset. Consistency is not enforced.
		 *
		 * @return position on the block grid
		 */
		long[] getGridPosition();
	}


	public interface Shard extends Block {

		// pos is relative to this shard
		ReadData getElementData(int[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, int[] pos);
	}


	public interface DataBlock<T> extends Block {

		/**
		 * Returns the data object held by this data block.
		 *
		 * @return data object
		 */
		T getData();

//		interface DataBlockFactory<T> {
//			DataBlock<T> createDataBlock(int[] blockSize, long[] gridPosition, T data);
//		}
	}


	public interface BlockCodec<T extends Block> {

		ReadData encode(T block) throws N5IOException;

		T decode(ReadData readData, long[] gridPosition) throws N5IOException;
	}


	public interface DataBlockCodec<T> extends BlockCodec<DataBlock<T>> {
	}


	static class N5ReaderImpl {

		/**
		 * Reads a {@link DataBlock}.
		 *
		 * @param pathName
		 * 		dataset path
		 * @param datasetAttributes
		 * 		the dataset attributes
		 * @param gridPosition
		 * 		the grid position
		 *
		 * @return the data block
		 *
		 * @throws N5Exception
		 * 		the exception
		 */
		DataBlock<?> readBlock(
				final String pathName,
				final DatasetAttributes datasetAttributes,
				final long... gridPosition) throws N5Exception {








			throw new UnsupportedOperationException();
		}
	}

}
