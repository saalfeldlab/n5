package org.janelia.saalfeldlab.n5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
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


	public interface BlockCodec<B extends Block> {

		ReadData encode(B block) throws N5IOException;

		B decode(ReadData readData, long[] gridPosition) throws N5IOException;
	}


	public interface DataBlockCodec<T> extends BlockCodec<DataBlock<T>> {
	}

	public static class BlockCodecs<T> {

		private final List<BlockCodec<?>> shardCodecs = new ArrayList<>();
		private final DataBlockCodec<T> dataBlockCodec;
		private final int[] dataBlockSize;

		public BlockCodecs(final DataBlockCodec<T> dataBlockCodec, final int[] dataBlockSize) {
			this.dataBlockCodec = dataBlockCodec;
			this.dataBlockSize = dataBlockSize;
		}

		public BlockCodecs(final BlockCodec<?> first, final BlockCodecs<T> others) {
			shardCodecs.add(first);
			shardCodecs.addAll(others.shardCodecs);
			dataBlockCodec = others.dataBlockCodec;
			dataBlockSize = others.dataBlockSize;
		}

		public int[] getDataBlockSize() {
			return dataBlockSize;
		}
	}

	public interface BlockCodecInfo extends CodecInfo {

		/**
		 * Chunk size of the elements in this block.
		 * That is, (1, ...) for DataBlockCodecInfo, respectively inner block size for ShardCodecInfo.
		 */
		int[] getChunkSize();

		/**
		 * Nested BlockCodec.
		 * ({@code null} for DataBlockCodec.
		 */
		BlockCodecInfo getInnerBlockCodecInfo();

		DataCodecInfo[] getInnerDataCodecInfos();

		<B extends Block> BlockCodec<B> create(int[] blockSize, DataCodecInfo... codecs);

		default <T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs)
		{
			final BlockCodecs<T> blockCodecs = getInnerBlockCodecInfo().createRecursive(
					dataType, getChunkSize(),
					getInnerDataCodecInfos());
			final BlockCodec<Block> shardCodec = create(blockSize, codecs);
			return new BlockCodecs<>(shardCodec, blockCodecs);
		}
	}

	private static DataCodec[] instantiate(final DataCodecInfo... codecInfos) {
		final DataCodec[] codecs = new DataCodec[codecInfos.length];
		Arrays.setAll(codecs, i -> codecInfos[i].create());
		return codecs;
	}

	public interface DataBlockCodecInfo extends BlockCodecInfo {

		@Override
		default <B extends Block> BlockCodec<B> create(int[] blockSize, DataCodecInfo... codecs) {
			// TODO: Fix BlockCodec hierarchy. It's a bad sign that we have to
			//       @Override this here. Maybe AbstractBlockCodecInfo extended by
			//       (Data)BlockCodecInfo and ShardCodecInfo
			throw new UnsupportedOperationException();
		}

		<T> DataBlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs);

		@Override
		default <T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {
			return new BlockCodecs<>(create(dataType, blockSize, codecs), blockSize);
		}
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
