package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.ProxyInputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public class VirtualShard<T> extends AbstractShard<T> {

	final private KeyValueAccess keyValueAccess;
	final private String path;

	public VirtualShard(final DatasetAttributes datasetAttributes, long[] gridPosition,
			final KeyValueAccess keyValueAccess, final String path) {

		super(datasetAttributes, gridPosition, null);
		this.keyValueAccess = keyValueAccess;
		this.path = path;
	}

	public VirtualShard(final DatasetAttributes datasetAttributes, long[] gridPosition) {

		this(datasetAttributes, gridPosition, null, null);
	}

	@SuppressWarnings("unchecked")
	public DataBlock<T> getBlock(InputStream in, long... blockGridPosition) throws IOException {

		ShardingCodec shardingCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		final Codec.BytesCodec[] codecs = shardingCodec.getCodecs();
		final Codec.ArrayCodec arrayCodec = shardingCodec.getArrayCodec();

		final ProxyInputStream proxyIn = new ProxyInputStream(in) {
			@Override
			public void close() {
				//nop
			}
		};
		final Codec.DataBlockInputStream dataBlockStream = arrayCodec.decode(datasetAttributes, blockGridPosition, proxyIn);

		final InputStream stream = Codec.decode(in, codecs);
		final DataBlock<T> dataBlock = dataBlockStream.allocateDataBlock();
		dataBlock.readData(dataBlockStream.getDataInput(stream));
		stream.close();

		return dataBlock;


	}

	@Override
	public List<DataBlock<T>> getBlocks()  {
		return getBlocks(IntStream.range(0, getNumBlocks()).toArray());
	}

	public List<DataBlock<T>> getBlocks(final int[] blockIndexes) {

		// will not contain nulls
		final ShardIndex index = getIndex();
		final ArrayList<DataBlock<T>> blocks = new ArrayList<>();

		if (index.isEmpty())
			return blocks;

		// sort index offsets
		// and keep track of relevant positions
		final long[] indexData = index.getData();
		List<long[]> sortedOffsets = Arrays.stream(blockIndexes)
				.mapToObj(i -> new long[]{indexData[i * 2], i})
				.filter(x -> x[0] != ShardIndex.EMPTY_INDEX_NBYTES)
				.sorted(Comparator.comparingLong(a -> ((long[])a)[0]))
				.collect(Collectors.toList());

		final int nd = getDatasetAttributes().getNumDimensions();
		long[] position = new long[nd];

		final int[] blocksPerShard = getDatasetAttributes().getBlocksPerShard();
		final long[] blockGridMin = IntStream.range(0, nd)
				.mapToLong(i -> blocksPerShard[i] * getGridPosition()[i])
				.toArray();

		long streamPosition = 0;
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path)) {
			try (final InputStream channelIn = lockedChannel.newInputStream()) {

				for (long[] offsetIndex : sortedOffsets) {

					final long offset = offsetIndex[0];
					if (offset < 0)
						continue;

					final long idx = offsetIndex[1];
					GridIterator.indexToPosition(idx, blocksPerShard, blockGridMin, position);

					channelIn.skip(offset - streamPosition);
					final long numBytes = index.getNumBytesByBlockIndex((int) idx);
					final BoundedInputStream bIs = BoundedInputStream.builder().setInputStream(channelIn)
							.setMaxCount(numBytes).get();

					blocks.add(getBlock(bIs, position.clone()));
					streamPosition = offset + numBytes;
				}
			}
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return blocks;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}

		return blocks;
	}

	@Override
	public DataBlock<T> getBlock(long... blockGridPosition) {

		final int[] relativePosition = getBlockPosition(blockGridPosition);
		if (relativePosition == null)
			throw new N5IOException("Attempted to read a block from the wrong shard.");

		final ShardIndex idx = getIndex();

		final long startByte = idx.getOffset(relativePosition);

		if (startByte == ShardIndex.EMPTY_INDEX_NBYTES )
			return null;

		final long size = idx.getNumBytes(relativePosition);
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path, startByte, size)) {
			try ( final InputStream channelIn = lockedChannel.newInputStream()) {
				final long[] blockPosInImg = getDatasetAttributes().getBlockPositionFromShardPosition(getGridPosition(), blockGridPosition);
				return getBlock( channelIn, blockPosInImg );
			}
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}
	}

	@Override
	public void writeBlock(final DataBlock<T> block) {

		final int[] relativePosition = getBlockPosition(block.getGridPosition());
		if (relativePosition == null)
			throw new N5IOException("Attempted to write block in the wrong shard.");

		final ShardIndex index = getIndex();
		long startByte = 0;
		try {
			startByte = keyValueAccess.size(path);
		} catch (N5Exception.N5NoSuchKeyException e) {
			startByte = index.getLocation() == ShardingCodec.IndexLocation.START ? index.numBytes() : 0;
		} catch (IOException e) {
			throw new N5IOException(e);
		}
		final long size = Long.MAX_VALUE - startByte;

		try (final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path, startByte, size)) {
			try ( final OutputStream channelOut = lockedChannel.newOutputStream()) {
				try (final CountingOutputStream out = new CountingOutputStream(channelOut)) {;
					writeBlock(out, datasetAttributes, block);

					/* Update and write the index to the shard*/
					index.set(startByte, out.getNumBytes(), relativePosition);
				}
			}
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to write block to shard " + path, e);
		}

		try {
			ShardIndex.write(index, keyValueAccess, path);
		} catch (IOException e) {
			throw new N5IOException("Failed to write index to shard " + path, e);
		}
	}

	<T> void writeBlock(
			final OutputStream out,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		ShardingCodec shardingCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		final Codec.BytesCodec[] codecs = shardingCodec.getCodecs();
		final Codec.ArrayCodec arrayCodec = shardingCodec.getArrayCodec();
		final Codec.DataBlockOutputStream dataBlockOutput = arrayCodec.encode(datasetAttributes, dataBlock, out);
		final OutputStream stream = Codec.encode(dataBlockOutput, codecs);

		dataBlock.writeData(dataBlockOutput.getDataOutput(stream));
		stream.close();
	}
	public ShardIndex createIndex() {

		// Empty index of the correct size
		return ((ShardingCodec)getDatasetAttributes().getArrayCodec()).createIndex(getDatasetAttributes());
	}

	@Override
	public ShardIndex getIndex() {

			index = createIndex();
		ShardIndex.read(keyValueAccess, path, index);

		return index;
	}

	static class CountingOutputStream extends OutputStream {
		private final OutputStream out;
		private long numBytes;

		public CountingOutputStream(OutputStream out) {
			this.out = out;
			this.numBytes = 0;
		}

		@Override
		public void write(int b) throws IOException {
			out.write(b);
			numBytes++;
		}

		@Override
		public void write(byte[] b) throws IOException {
			out.write(b);
			numBytes += b.length;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			out.write(b, off, len);
			numBytes += len;
		}

		@Override
		public void flush() throws IOException {
			out.flush();
		}

		@Override
		public void close() throws IOException {

		}

		public long getNumBytes() {
			return numBytes;
		}
	}
}
