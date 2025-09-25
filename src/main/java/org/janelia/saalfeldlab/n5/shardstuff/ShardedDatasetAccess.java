package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;

public class ShardedDatasetAccess<T> implements DatasetAccess<T> {

	private final NestedGrid grid;
	private final BlockCodec<?>[] codecs;

	public ShardedDatasetAccess(final NestedGrid grid, final BlockCodec<?>[] codecs) {
		this.grid = grid;
		this.codecs = codecs;
	}

	public NestedGrid getGrid() {
		return grid;
	}

	@Override
	public DataBlock<T> readBlock(final PositionValueAccess kva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = new NestedPosition(grid, gridPosition);
		return readBlockRecursive(kva.get(position.key()), position, grid.numLevels() - 1);
	}

	private DataBlock<T> readBlockRecursive(
			final ReadData readData,
			final NestedPosition position,
			final int level) {
		if (readData == null) {
			return null;
		} else if (level == 0) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			return codec.decode(readData, position.absolute(0));
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final RawShard shard = codec.decode(readData, position.absolute(level)).getData();
			return readBlockRecursive(shard.getElementData(position.relative(level - 1)), position, level - 1);
		}
	}

	@Override
	public void writeBlock(final PositionValueAccess pva, final DataBlock<T> dataBlock) throws N5IOException {
		final NestedPosition position = new NestedPosition(grid, dataBlock.getGridPosition());
		final long[] key = position.key();

		// need to read the shard anyway, and currently (Sept 24 2025)
		// have no way to tell if they key exist from what is in this method except to attempt
		// to materialize and catch the N5NoSuchKeyException 
		ReadData existingData = null;
		try {
			existingData = pva.get(key);
			if (existingData != null)
				existingData.materialize();
		} catch (N5NoSuchKeyException e) {}

		final ReadData modifiedData = writeBlockRecursive(existingData, dataBlock, position, grid.numLevels() - 1);
		pva.put(key, modifiedData);
	}

	private ReadData writeBlockRecursive(
			final ReadData existingReadData,
			final DataBlock<T> dataBlock,
			final NestedPosition position,
			final int level) {
		if (level == 0) {
			@SuppressWarnings("unchecked")
			final BlockCodec<T> codec = (BlockCodec<T>) codecs[0];
			return codec.encode(dataBlock);
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = existingReadData == null ?
					new RawShard(grid.relativeBlockSize(level)) :
					codec.decode(existingReadData, gridPos).getData();
			final long[] elementPos = position.relative(level - 1);
			final ReadData existingElementData = (level == 1)
					? null // if level == 1, we don't need to extract the nested (DataBlock<T>) ReadData because it will be overridden anyway
					: shard.getElementData(elementPos);
			final ReadData modifiedElementData = writeBlockRecursive(existingElementData, dataBlock, position, level - 1);
			shard.setElementData(modifiedElementData, elementPos);
			return codec.encode(new RawShardDataBlock(gridPos, shard));
		}
	}

	@Override
	public void deleteBlock(final PositionValueAccess kva, final long[] gridPosition) throws N5IOException {
		final NestedPosition position = new NestedPosition(grid, gridPosition);
		final long[] key = position.key();
		if (grid.numLevels() == 1) {
			// for non-sharded dataset, don't bother getting the value, just remove the key.
			kva.remove(key);
		} else {
			final ReadData existingData = kva.get(key);
			final ReadData modifiedData = deleteBlockRecursive(existingData, position, grid.numLevels() - 1);
			if (modifiedData == null) {
				kva.remove(key);
			} else if (modifiedData != existingData) {
				kva.put(key, modifiedData);
			}
		}
	}

	private ReadData deleteBlockRecursive(
			final ReadData existingReadData,
			final NestedPosition position,
			final int level) {
		if (level == 0 || existingReadData == null) {
			return null;
		} else {
			@SuppressWarnings("unchecked")
			final BlockCodec<RawShard> codec = (BlockCodec<RawShard>) codecs[level];
			final long[] gridPos = position.absolute(level);
			final RawShard shard = codec.decode(existingReadData, gridPos).getData();
			final long[] elementPos = position.relative(level - 1);
			final ReadData existingElementData = shard.getElementData(elementPos);
			if (existingElementData == null) {
				// The DataBlock (or the whole nested shard containing it) does not exist.
				// This shard remains unchanged.
				return existingReadData;
			} else {
				final ReadData modifiedElementData = deleteBlockRecursive(existingElementData, position, level - 1);
				if (modifiedElementData == existingElementData) {
					// The nested shard was not modified.
					// This shard remains unchanged.
					return existingReadData;
				}
				shard.setElementData(modifiedElementData, elementPos);
				if (modifiedElementData == null) {
					// The DataBlock or nested shard was removed.
					// Check whether this shard becomes empty.
					if (shard.index().allElementsNull()) {
						// This shard is empty and should be removed.
						return null;
					}
				}
				return codec.encode(new RawShardDataBlock(gridPos, shard));
			}
		}
	}

	public static <T> ShardedDatasetAccess<T> create(
			final DataType dataType,
			int[] blockSize,
			BlockCodecInfo blockCodecInfo,
			DataCodecInfo[] dataCodecInfos
	) {
		final int m = nestingDepth(blockCodecInfo);

		// There are m codecs: 1 DataBlock codecs, and m-1 shard codecs.
		// The inner-most codec (the DataBlock codec) is at index 0.
		final int[][] blockSizes = new int[m][];

		// NestedGrid validates block sizes, so instantiate it before creating the blockCodecs
		// blockCodecInfo.create below could fail unexpecedly with invalid blockSizes
		// so validate first
		blockSizes[m-1] = blockSize;
		BlockCodecInfo tmpInfo = blockCodecInfo;
		for (int l = m - 1; l > 0; --l) {
			final ShardCodecInfo info = (ShardCodecInfo) tmpInfo;
			blockSizes[l-1] = info.getInnerBlockSize();
			tmpInfo = info.getInnerBlockCodecInfo();
		}

		final NestedGrid grid = new NestedGrid(blockSizes);
		final BlockCodec<?>[] blockCodecs = new BlockCodec[m];
		for (int l = m - 1; l >= 0; --l) {
			blockCodecs[l] = blockCodecInfo.create(dataType, blockSizes[l], dataCodecInfos);
			if (l > 0) {
				final ShardCodecInfo info = (ShardCodecInfo) blockCodecInfo;
				blockCodecInfo = info.getInnerBlockCodecInfo();
				dataCodecInfos = info.getInnerDataCodecInfos();
			}
		}

		return new ShardedDatasetAccess<>(grid, blockCodecs);
	}

	private static int nestingDepth(BlockCodecInfo info) {
		if (info instanceof ShardCodecInfo) {
			return 1 + nestingDepth(((ShardCodecInfo) info).getInnerBlockCodecInfo());
		} else {
			return 1;
		}
	}
}
