package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.util.Position;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InMemoryShard<T> extends AbstractShard<T> {

	/** Map {@link DataBlock#getGridPosition} as hashable {@link Position} to the block */
	private final Map<Position, DataBlock<T>> blocks;

	public InMemoryShard(final DatasetAttributes datasetAttributes, final long[] shardPosition) {

		this(datasetAttributes, shardPosition, null);
	}

	public InMemoryShard(
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition,
			ShardIndex index) {

		super(datasetAttributes, gridPosition, index);
		blocks = new TreeMap<>();
	}

	private void storeBlock(DataBlock<T> block) {

		blocks.put(Position.wrap(block.getGridPosition()), block);
	}

	@Override
	public DataBlock<T> getBlock(int... blockGridPosition) {

		return blocks.get(Position.wrap(blockGridPosition));
	}

	/**
	 * Add the {@code block} to this shard. If the block is not contained in this shard, do not add it.
	 *
	 * @param block to add the shard
	 * @return whether the block was added
	 */
	public boolean addBlock(DataBlock<T> block) {

		final long[] shardPositionForBlock = datasetAttributes.getShardPositionForBlock(block.getGridPosition());
		if (!Arrays.equals(shardPositionForBlock, getGridPosition()))
			return false;

		storeBlock(block);
		return true;
	}

	@Override
	public List<DataBlock<T>> getBlocks() {

		return new ArrayList<>(blocks.values());
	}

	@Override
	public ShardIndex getIndex() {

//		return index = index != null ? index : createIndex();

		// TODO
		return index;
	}

	public static <T> InMemoryShard<T> fromShard(Shard<T> shard) {

		if (shard == null)
			return null;

		if (shard instanceof InMemoryShard)
			return (InMemoryShard<T>)shard;

		final InMemoryShard<T> inMemoryShard = new InMemoryShard<T>(
				shard.getDatasetAttributes(),
				shard.getGridPosition());

		shard.forEach(inMemoryShard::addBlock);
		return inMemoryShard;
	}
}
