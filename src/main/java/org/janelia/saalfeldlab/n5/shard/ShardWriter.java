package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShardWriter {

	private static final int BYTES_PER_LONG = 8;

	private final List<DataBlock<?>> blocks;

	private final ShardedDatasetAttributes datasetAttributes;

	private ByteBuffer blockSizes;

	private ByteBuffer blockIndexes;

	private ShardIndex indexData;

	private List<byte[]> blockBytes;

	public ShardWriter(final ShardedDatasetAttributes datasetAttributes) {

		blocks = new ArrayList<>();
		this.datasetAttributes = datasetAttributes;
	}

	public void reset() {

		blocks.clear();
		blockSizes = null;
		blockBytes.clear();

		indexData = null;
	}

	public void addBlock(final DataBlock<?> block) {

		blocks.add(block);
	}

	public void write(final OutputStream out) throws IOException {

		// TODO need codecs

		// prepareForWriting();
		// if (datasetAttributes.getShardingConfiguration().getIndexLocation()) {
		// writeIndexes(out);
		// writeBlocks(out);
		// } else {
		// writeBlocks(out);
		// writeIndexes(out);
		// }

		prepareForWritingDataBlock();
		if (datasetAttributes.getIndexLocation() == ShardingCodec.IndexLocation.START) {
			writeIndexBlock(out);
			writeBlocks(out);
		} else {
			writeBlocks(out);
			writeIndexBlock(out);
		}
	}

	private void prepareForWritingDataBlock() throws IOException {

		// final ShardingProperties shardProps = new ShardingProperties(datasetAttributes);
		// indexData = new ShardIndexDataBlock(shardProps.getIndexDimensions());

		indexData = datasetAttributes.createIndex();
		blockBytes = new ArrayList<>();
		long cumulativeBytes = 0;
		final long[] shardPosition = new long[1];
		for (int i = 0; i < blocks.size(); i++) {

			try (final ByteArrayOutputStream blockOut = new ByteArrayOutputStream()) {
				DefaultBlockWriter.writeBlock(blockOut, datasetAttributes, blocks.get(i));
				System.out.println(String.format("block %d is %d bytes", i, blockOut.size()));

				shardPosition[0] = i;
				indexData.set(cumulativeBytes, blockOut.size(), shardPosition);
				cumulativeBytes += blockOut.size();

				blockBytes.add(blockOut.toByteArray());
			}
		}

		System.out.println(Arrays.toString(indexData.getData()));
	}

	private void prepareForWriting() throws IOException {

		blockSizes = ByteBuffer.allocate(BYTES_PER_LONG * blocks.size());
		blockIndexes = ByteBuffer.allocate(BYTES_PER_LONG * blocks.size());
		blockBytes = new ArrayList<>();
		long cumulativeBytes = 0;
		for (int i = 0; i < blocks.size(); i++) {

			try (final ByteArrayOutputStream blockOut = new ByteArrayOutputStream()) {

				DefaultBlockWriter.writeBlock(blockOut, datasetAttributes, blocks.get(i));
				System.out.println(String.format("block %d is %d bytes", i, blockOut.size()));

				blockIndexes.putLong(cumulativeBytes);
				blockSizes.putLong(blockOut.size());
				cumulativeBytes += blockOut.size();

				blockBytes.add(blockOut.toByteArray());
			}
		}
	}

	private void writeBlocks(final OutputStream out) throws IOException {

		for (final byte[] bytes : blockBytes)
			out.write(bytes);
	}

	private void writeIndexes(final OutputStream out) throws IOException {

		out.write(blockSizes.array());
	}

	private void writeIndexBlock(final OutputStream out) throws IOException {

		final DataOutputStream dos = new DataOutputStream(out);
		for (final long l : indexData.getData())
			dos.writeLong(l);
	}

}
