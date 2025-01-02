package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;

public class InMemoryShard<T> extends AbstractShard<T> {

	private List<DataBlock<T>> blocks;
	
	private ShardIndexBuilder indexBuilder;
	
	/*
	 * TODO:
	 * Use morton- or c-ording instead of writing blocks out in the order they're added?
	 * (later)
	 */

	public InMemoryShard(final ShardedDatasetAttributes datasetAttributes, final long[] gridPosition) {

		this( datasetAttributes, gridPosition, null);
		indexBuilder = new ShardIndexBuilder(this);
		indexBuilder.indexLocation(datasetAttributes.getIndexLocation());
	}

	public InMemoryShard(final ShardedDatasetAttributes datasetAttributes, final long[] gridPosition,
			ShardIndex index) {

		super(datasetAttributes, gridPosition, index);
		blocks = new ArrayList<>();
	}

	@Override
	public void writeBlock(DataBlock<T> block) {
		
		addBlock(block);
	}

	public void addBlock(DataBlock<T> block) {

		blocks.add(block);
	}

	public int numBlocks() {

		return blocks.size();
	}
	
	public DataBlock<T> getBlock(int i) {

		return blocks.get(i);
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
			writeShardEnd(out, this);
		else
			writeShardStart(out, this);
	}

	public static <T> void writeShard(final OutputStream out, final Shard<T> shard) throws IOException {

		fromShard(shard).write(out);
	}

	public static <T> InMemoryShard<T> fromShard(Shard<T> shard) {

		if (shard instanceof InMemoryShard)
			return (InMemoryShard<T>) shard;

		final InMemoryShard<T> inMemoryShard = new InMemoryShard<T>(shard.getDatasetAttributes(),
				shard.getGridPosition());

		shard.forEach(blk -> inMemoryShard.addBlock(blk));
		return inMemoryShard;
	}

	protected static <T> void writeShardEndStream(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final ShardedDatasetAttributes datasetAttributes = shard.getDatasetAttributes();

		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.END);

		final CountingOutputStream cout = new CountingOutputStream(out);
		
		long offset = 0;
		for (int i = 0; i < shard.numBlocks(); i++) {

			final DataBlock<T> block = shard.getBlock(i);
			DefaultBlockWriter.writeBlock(cout, datasetAttributes, block);
			
			indexBuilder.addBlock( block.getGridPosition(), offset);
			offset = cout.getByteCount();
		}

		final ShardIndex index = indexBuilder.build();
		DefaultBlockWriter.writeBlock(out, datasetAttributes, index);
	}

	protected static <T> void writeShardEnd(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final ShardedDatasetAttributes datasetAttributes = shard.getDatasetAttributes();

		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.END);
		indexBuilder.setCodecs(datasetAttributes.getShardingCodec().getIndexCodecs());

		for (int i = 0; i < shard.numBlocks(); i++) {

			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			final DataBlock<T> block = shard.getBlock(i);
			DefaultBlockWriter.writeBlock(os, datasetAttributes, block);

			indexBuilder.addBlock(block.getGridPosition(), os.size());
			out.write(os.toByteArray());
		}

		ShardIndex.write(indexBuilder.build(), out);
	}

	protected static <T> void writeShardStart(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final ShardedDatasetAttributes datasetAttributes = shard.getDatasetAttributes();
		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.START);
		indexBuilder.setCodecs(datasetAttributes.getShardingCodec().getIndexCodecs());

		final List<byte[]> blockData = new ArrayList<>(shard.numBlocks());
		for (int i = 0; i < shard.numBlocks(); i++) {

			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			final DataBlock<T> block = shard.getBlock(i);
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
