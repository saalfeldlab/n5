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
		 * Grid position (0, ...) is the origin of the dataset, that is, grid positions are not relative to the containing shard etc.
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
		ReadData getElementData(long[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, long[] pos);
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

		/**
		 * {@code gridPosition} is with respect to the level of the block (Shard, nested Shard, DataBlock, ...).
		 * If a block has gridPosition (i, ...), then the adjacent block (in X) has position (i+1, ...).
		 * Grid position (0, ...) is the origin of the dataset, that is, grid positions are not relative to the containing shard etc.
		 */
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

		ShardCodec create(int[] blockSize, DataCodecInfo... codecs);

		@Override
		default <T> BlockCodecs<T> createRecursive(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {
			final ShardCodec shardCodec = create(blockSize, codecs);
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
		private final DataBlockCodec<T> dataBlockCodec;


		// size of shard at a given level in units of DataBlocks
		private final List<int[]> chunkSizesInDataBlocks = new ArrayList<>();

		// number of nested elements in shard at a given level
		private final List<int[]> relativeChunkSizes = new ArrayList<>();

		private final int[] dataBlockSize;

		public BlockCodecs(final DataBlockCodec<T> dataBlockCodec, final int[] dataBlockSize) {
			this.dataBlockCodec = dataBlockCodec;
			this.dataBlockSize = dataBlockSize;

			final int[] gridSize = new int[dataBlockSize.length];
			Arrays.fill(gridSize, 1);
			chunkSizesInDataBlocks.add(gridSize);
		}

		/**
		 * @param shardSize in pixels
		 */
		public BlockCodecs(final ShardCodec codec, final int[] shardSize, final BlockCodecs<T> others) {
			this(others.dataBlockCodec, others.dataBlockSize);
			shardCodecs.add(codec);
			shardCodecs.addAll(others.shardCodecs);

			// TODO: verify that shardSize is integer multiple of dataBlockSize.
			// TODO: verify that shardSize is integer multiple of nested shardSize

			final int[] gridSize = new int[shardSize.length];
			Arrays.setAll(gridSize, d -> shardSize[d] / dataBlockSize[d]);
			chunkSizesInDataBlocks.add(gridSize);
			chunkSizesInDataBlocks.addAll(others.chunkSizesInDataBlocks);

			final int[] relativeChunkSize = new int[shardSize.length];
			final int[] nestedGridSize = others.chunkSizesInDataBlocks.get(0);
			Arrays.setAll(relativeChunkSize, d -> gridSize[d] / nestedGridSize[d]);
			relativeChunkSizes.add(relativeChunkSize);
			relativeChunkSizes.addAll(others.relativeChunkSizes);
		}

		public int[] getDataBlockSize() {
			return dataBlockSize;
		}



		// TODO: Decide whether to use absolute of relative NestedBlockPosition.
		//       (For decoding we need both:
		//       	shard.getElementData(RELATIVE_GRID_POSITION),
		//       	(shard|dataBlock)Codec.decode(data, ABSOLUTE_GRID_POSITION)
		//       Maybe NestedBlockPosition should just contain both?

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

		public NestedBlockPosition relativeToAbsolute(final NestedBlockPosition nestedPos) {

		}







		public Shard getShard(ReadData data, final NestedBlockPosition nestedPos) {

			// TODO: validation: nestedPos should refer to a Shard (not a DataBlock)

			// TODO: handle missing shards

			final int depth = nestedPos.depth();
			final int n = nestedPos.numDimensions();

			final long[] gridOffset = new long[n];
			for (int i = 0; i < depth; ++i) {
				final long[] relativeGridPos = nestedPos.position(i);

				final long[] gridPosition = new long[n];
				Arrays.setAll(gridPosition, d -> gridOffset[d] + relativeGridPos[d]);

				final Shard shard = shardCodecs.get(i).decode(data, gridPosition);
				if ( i == depth - 1 ) {
					return shard;
				}
				data = shard.getElementData(relativeGridPos);

				final int[] relativeGridSize = relativeChunkSizes.get(i);
				Arrays.setAll(gridOffset, d -> gridPosition[d] * relativeGridSize[d]);
			}

			throw new IllegalStateException(); // we should never end up here
		}

		public DataBlock<T> extractDataBlock(ReadData data, final NestedBlockPosition nestedPos) {

			// TODO: validation: nestedPos should refer to a DataBlock (not a Shard)

			// TODO: handle missing shards / blocks

			final int depth = nestedPos.depth();
			final int n = nestedPos.numDimensions();

			final long[] gridOffset = new long[n];
			for (int i = 0; i < depth; ++i) {
				final long[] relativeGridPos = nestedPos.position(i);

				final long[] gridPosition = new long[n];
				Arrays.setAll(gridPosition, d -> gridOffset[d] + relativeGridPos[d]);

				if (i == depth - 1) {
					return dataBlockCodec.decode(data, gridPosition);
				} else {
					final Shard shard = shardCodecs.get(i).decode(data, gridPosition);
					data = shard.getElementData(relativeGridPos);
				}

				final int[] relativeGridSize = relativeChunkSizes.get(i);
				Arrays.setAll(gridOffset, d -> gridPosition[d] * relativeGridSize[d]);
			}

			throw new IllegalStateException(); // we should never end up here
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

		public int numDimensions() {
			return nested[0].length;
		}

		public long[] position(final int level) {
			return nested[level];
		}

		public long[] keyPosition() {
			return position(0);
		}

		public NestedBlockPosition prefix(final int depth) {
			// TODO: implement
			throw new UnsupportedOperationException("TODO");
		}
	}


	// TODO: Implement NestedBlockPositions that recursively groups blocks under the same prefix.
	//       This would be the input to





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
				return blockCodecs.extractDataBlock(keyData, pos);
			} catch (N5Exception.N5NoSuchKeyException e) {
				return null;
			}
		}


	}














	private static DataCodec[] instantiate(final DataCodecInfo... codecInfos) {
		final DataCodec[] codecs = new DataCodec[codecInfos.length];
		Arrays.setAll(codecs, i -> codecInfos[i].create());
		return codecs;
	}
}
