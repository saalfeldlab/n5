package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
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

	@Override
	public void writeShard() {

	}
	
	public static <T> void writeShard(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final ShardedDatasetAttributes datasetAttributes = shard.getDatasetAttributes();
		
		if( shard.getIndex().getLocation() == IndexLocation.END)
			writeShardEnd( out, shard);
		else
			writeShardStart( out, shard);

	}

	protected static <T> void writeShardEnd(
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
			
			indexBuilder.addBLock( block.getGridPosition(), offset);
			offset = cout.getByteCount();
		}

		final ShardIndex index = indexBuilder.build();
		DefaultBlockWriter.writeBlock(out, datasetAttributes, index);
	}

	protected static <T> void writeShardStart(
			final OutputStream out,
			InMemoryShard<T> shard ) throws IOException {

		final ShardedDatasetAttributes datasetAttributes = shard.getDatasetAttributes();
		final ShardIndexBuilder indexBuilder = new ShardIndexBuilder(shard);
		indexBuilder.indexLocation(IndexLocation.START);

		long offset = 0;
		final List<byte[]> blockData = new ArrayList<>(shard.numBlocks());
		for (int i = 0; i < shard.numBlocks(); i++) {

			final DataBlock<T> block = shard.getBlock(i);

			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			DefaultBlockWriter.writeBlock(os, datasetAttributes, block);
			final byte[] data = os.toByteArray();

			blockData.add(data);
			indexBuilder.addBLock( block.getGridPosition(), offset);
			offset += data.length;
		}		
		
		final ShardIndex index = indexBuilder.build();
		try {
			DefaultBlockWriter.writeBlock(out, datasetAttributes, index);

			for( byte[] data : blockData )
				out.write(data);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

}
