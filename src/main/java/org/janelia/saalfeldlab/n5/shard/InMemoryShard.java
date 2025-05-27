package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InMemoryShard<T> extends AbstractShard<T> {

	/* Map of a hash of the DataBlocks `gridPosition` to the block */
	private final Map<Position, DataBlock<T>> blocks;
	private ShardIndexBuilder indexBuilder;

	//TODO delegated shard constructor? Or new class?

	public InMemoryShard(final DatasetAttributes datasetAttributes, final long[] shardPosition) {

		this(datasetAttributes, shardPosition, null);
		indexBuilder = new ShardIndexBuilder(this);
		final IndexLocation indexLocation = ((ShardingCodec)datasetAttributes.getArrayCodec()).getIndexLocation();
		indexBuilder.indexLocation(indexLocation);
	}

	public InMemoryShard(final DatasetAttributes datasetAttributes, final long[] gridPosition,
			ShardIndex index) {

		super(datasetAttributes, gridPosition, index);
		blocks = new TreeMap<>();
	}

	private void storeBlock(DataBlock<T> block) {

		blocks.put(Position.wrap(block.getGridPosition()), block);
	}

	/*
	 * Returns the {@link DataBlock} given a block grid position.
	 * <p>
	 * The block grid position is relative to the image, not relative to this shard.
	 */
	@Override public DataBlock<T> getBlock(long... blockGridPosition) {

		return blocks.get(Position.wrap(blockGridPosition));
	}

	public void addBlock(DataBlock<T> block) {

		storeBlock(block);
	}

	@Override
	public List<DataBlock<T>> getBlocks() {

		return new ArrayList<>(blocks.values());
	}

	@Override
	public ShardIndex getIndex() {

		if (index != null)
			return index;
		else
			return indexBuilder.build();
	}

	public static <T> InMemoryShard<T> fromShard(Shard<T> shard) {

		if (shard instanceof InMemoryShard)
			return (InMemoryShard<T>)shard;

		final InMemoryShard<T> inMemoryShard = new InMemoryShard<T>(
				shard.getDatasetAttributes(),
				shard.getGridPosition());

		shard.forEach(inMemoryShard::addBlock);
		return inMemoryShard;
	}
}
