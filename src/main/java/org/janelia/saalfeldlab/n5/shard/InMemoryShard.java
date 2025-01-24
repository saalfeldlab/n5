package org.janelia.saalfeldlab.n5.shard;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.SplitByteBufferedData;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
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
	public <A extends DatasetAttributes & ShardParameters> InMemoryShard(final A datasetAttributes, final long[] shardPosition) {

		this( datasetAttributes, shardPosition, null);
		indexBuilder = new ShardIndexBuilder(this);
		final IndexLocation indexLocation = ((ShardingCodec)datasetAttributes.getArrayCodec()).getIndexLocation();
		indexBuilder.indexLocation(indexLocation);
	}

	public <A extends DatasetAttributes & ShardParameters> InMemoryShard(final A datasetAttributes, final long[] gridPosition,
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

		if( index != null )
			return index;
		else
			return indexBuilder.build();
	}

	public void write(final OutputStream out) throws IOException {

		if (indexLocation() == IndexLocation.END)
			writeShardEndStream(out, this);
		else
			writeShardStart(out, this);
	}

	public static <T> InMemoryShard<T> fromShard(Shard<T> shard) {

		if (shard instanceof InMemoryShard)
			return (InMemoryShard<T>) shard;

		final InMemoryShard<T> inMemoryShard = new InMemoryShard<T>(
				shard.getDatasetAttributes(),
				shard.getGridPosition());

		shard.forEach(blk -> inMemoryShard.addBlock(blk));
		return inMemoryShard;
	}

	protected static <T, A extends DatasetAttributes & ShardParameters> void writeShardEndStream(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final A datasetAttributes = shard.getDatasetAttributes();

		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.END);
		ShardingCodec shardingCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		indexBuilder.setCodecs(shardingCodec.getIndexCodecs());

		// Necessary to stop `close()` when writing blocks from closing out base OutputStream
		final ProxyOutputStream nop = new ProxyOutputStream(out) {
			@Override public void close() {
				//nop
			}
		};

		final CountingOutputStream cout = new CountingOutputStream(nop);

		long bytesWritten = 0;
		for (DataBlock<T> block : shard.getBlocks()) {
			DefaultBlockWriter.writeBlock(cout, datasetAttributes, block);
			final long size = cout.getByteCount() - bytesWritten;
			bytesWritten = cout.getByteCount();

			indexBuilder.addBlock( block.getGridPosition(), size);
		}

		ShardIndex.write(indexBuilder.build(), out);
	}

	protected static <T, A extends DatasetAttributes & ShardParameters> void writeShardEnd(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final A datasetAttributes = shard.getDatasetAttributes();

		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.END);
		final DeterministicSizeCodec[] indexCodecs = ((ShardingCodec)datasetAttributes.getArrayCodec()).getIndexCodecs();
		indexBuilder.setCodecs(indexCodecs);

		for (DataBlock<T> block : shard.getBlocks()) {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			DefaultBlockWriter.writeBlock(os, datasetAttributes, block);

			indexBuilder.addBlock(block.getGridPosition(), os.size());
			out.write(os.toByteArray());
		}

		ShardIndex.write(indexBuilder.build(), out);
	}

	protected static <T, A extends DatasetAttributes & ShardParameters> void writeShardStart(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final A datasetAttributes = shard.getDatasetAttributes();
		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.START);
		indexBuilder.setCodecs(((ShardingCodec)datasetAttributes.getArrayCodec()).getIndexCodecs());

		final SplitByteBufferedData splitData = new SplitByteBufferedData();
		try (final OutputStream blocksOut = splitData.newOutputStream()) {
			for (DataBlock<T> block : shard.getBlocks()) {

			}
		}


		final List<byte[]> blockData = new ArrayList<>(shard.numBlocks());
		for (DataBlock<T> block : shard.getBlocks()) {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			DefaultBlockWriter.writeBlock(os, datasetAttributes, block);

			blockData.add(os.toByteArray());
			indexBuilder.addBlock(block.getGridPosition(), os.size());
		}		

		try {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			ShardIndex.write(indexBuilder.build(), os);
			out.write(os.toByteArray());

			for( byte[] data : blockData )
				out.write(data);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
