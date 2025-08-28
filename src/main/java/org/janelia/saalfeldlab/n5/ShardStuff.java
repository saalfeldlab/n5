package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class ShardStuff {

	private static DataCodec[] instantiate(final DataCodecInfo... codecInfos) {
		final DataCodec[] codecs = new DataCodec[codecInfos.length];
		Arrays.setAll(codecs, i -> codecInfos[i].create());
		return codecs;
	}


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

	public interface ShardCodec extends BlockCodec<Shard> {
	}

	public interface DataBlockCodec<T> extends BlockCodec<DataBlock<T>> {
	}

	public interface BlockCodecInfo extends CodecInfo {

		<T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs);
	}

	public interface ShardCodecInfo extends BlockCodecInfo {

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

		@Override
		default <T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {
			final BlockCodec<Block> shardCodec = create(blockSize, codecs);
			final BlockCodecs<T> blockCodecs = getInnerBlockCodecInfo().createRecursive(
					dataType, getChunkSize(),
					getInnerDataCodecInfos());
			return new BlockCodecs<>(shardCodec, blockSize, blockCodecs);
		}
	}

	public interface DataBlockCodecInfo extends BlockCodecInfo {

		<T> DataBlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs);

		@Override
		default <T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {
			return new BlockCodecs<>(create(dataType, blockSize, codecs), blockSize);
		}
	}


	public static class BlockCodecs<T> {

		private final List<ShardCodec> shardCodecs = new ArrayList<>();

		private final List<int[]> chunkSizesInDataBlocks = new ArrayList<>();

		private final DataBlockCodec<T> dataBlockCodec;

		private final int[] dataBlockSize;

		public BlockCodecs(final DataBlockCodec<T> dataBlockCodec, final int[] dataBlockSize) {
			this.dataBlockCodec = dataBlockCodec;
			this.dataBlockSize = dataBlockSize;
		}

		public BlockCodecs(final ShardCodec codec, final int[] shardSize, final BlockCodecs<T> others) {
			this(others.dataBlockCodec, others.dataBlockSize);
			shardCodecs.add(codec);
			shardCodecs.addAll(others.shardCodecs);

			final int[] gridSize = new int[shardSize.length];
			Arrays.setAll(gridSize, d -> shardSize[d] / dataBlockSize[d]);
			chunkSizesInDataBlocks.add(gridSize);
			// TODO: verify shardSize is integer multiple of dataBlockSize.
			// TODO: verify size is integer multiple of nested chunkSizeInDataBlocks.

		}

		public int[] getDataBlockSize() {
			return dataBlockSize;
		}

		public NestedBlockPosition getNestedDataBlockPosition(final long[] gridPos) {
			if (shardCodecs.isEmpty()) {
				return new NestedBlockPosition(new long[][] {gridPos});
			}

			final int n = gridPos.length;
			final int m = shardCodecs.size() + 1;

			final long[][] nested = new long[m][n];
			final long[] pos = nested[m - 1];
			Arrays.setAll(pos, d -> gridPos[d]);
			for (int i = 0; i < m - 1; ++i) {
				final int[] gridSize = chunkSizesInDataBlocks.get(i);
				for (int d = 0; d < n; ++d) {
					nested[i][d] = pos[d] / gridSize[d];
					pos[d] = pos[d] % gridSize[d];
				}
			}

			return new NestedBlockPosition(nested);
		}

		public DataBlock<T> decodeDataBlock(final ReadData keyData, final NestedBlockPosition pos) {





			throw new UnsupportedOperationException();	
		}
	}

	// TODO: Implement Comparable so that we can sort and aggregate for N5Reader.readBlocks(...).
	//       For nested = {X,Y,Z} compare by X, then Y, then Z.
	//       For X = {x,y,z} compare by z, then y, then x. (flattening order)
	public static class NestedBlockPosition {

		private final long[][] nested;

		NestedBlockPosition(final long[][] nested) {
			// TODO: validation: nested != null, at least one level
			this.nested = nested;
		}

		public int depth() {
			return nested.length;
		}

		public long[] relativePosition(final int level) {
			return nested[level];
		}

		public long[] keyPosition() {
			return relativePosition(0);
		}
	}


	// DUMMY
	static abstract class N5ReaderImpl implements GsonKeyValueN5Reader {


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
				final BlockCodecs<?> blockCodecs, // TODO: get from DatasetAttributes
				final long... gridPosition) throws N5Exception {

			final NestedBlockPosition pos = blockCodecs.getNestedDataBlockPosition(gridPosition);
			final String path = absoluteDataBlockPath(N5URI.normalizeGroupPath(pathName), pos.keyPosition());

			try {
				final ReadData keyData = getKeyValueAccess().createReadData(path);
				return blockCodecs.decodeDataBlock(keyData, pos);
			} catch (N5Exception.N5NoSuchKeyException e) {
				return null;
			}

			throw new UnsupportedOperationException();
		}


	}

}
