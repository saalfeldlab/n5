package org.janelia.saalfeldlab.n5.shard;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.SplitByteBufferedData;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InMemoryShard<T> extends AbstractShard<T> {

	/* Map of a hash of the DataBlocks `gridPosition` to the block */
	private final Map<Position, DataBlock<T>> blocks;
	private ShardIndexBuilder indexBuilder;

	/*
	 * TODO:
	 * Use morton- or c-ording instead of writing blocks out in the order they're added?
	 * (later)
	 */
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

	@Override
	public void writeBlock(DataBlock<T> block) {

		addBlock(block);
	}

	public void addBlock(DataBlock<T> block) {

		storeBlock(block);
	}

	public int numBlocks() {

		return blocks.size();
	}

	@Override
	public List<DataBlock<T>> getBlocks() {

		return new ArrayList<>(blocks.values());
	}

	public List<DataBlock<T>> getBlocks( int[] blockIndexes ) {

		final ArrayList<DataBlock<T>> out = new ArrayList<>();
		final int[] blocksPerShard = getDatasetAttributes().getBlocksPerShard();

		long[] position = new long[ getSize().length ];
		for( int idx : blockIndexes ) {
			GridIterator.indexToPosition(idx, blocksPerShard, position);
			DataBlock<T> blk = getBlock(position);
			if( blk != null )
				out.add(blk);
		}
		return out;
	}
	protected IndexLocation indexLocation() {

		if (index != null)
			return index.getLocation();
		else
			return indexBuilder.getLocation();
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

		shard.forEach(blk -> inMemoryShard.addBlock(blk));
		return inMemoryShard;
	}
}
