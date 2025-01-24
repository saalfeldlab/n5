package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.SplitableData;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.util.GridIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VirtualShard<T> extends AbstractShard<T> {

	private final SplitableData splitableData;

	public VirtualShard(
			final DatasetAttributes datasetAttributes,
			long[] gridPosition,
			final SplitableData splitableData) {

		super(datasetAttributes, gridPosition, null);
		this.splitableData = splitableData;
	}

	@SuppressWarnings("unchecked")
	public DataBlock<T> getBlock(InputStream in, long... blockGridPosition) throws IOException {

		ShardingCodec shardingCodec = (ShardingCodec)datasetAttributes.getArrayCodec();
		final Codec.BytesCodec[] blockCodecs = shardingCodec.getCodecs();
		final Codec.ArrayCodec blockArrayCodec = shardingCodec.getArrayCodec();

		final Codec.DataBlockInputStream dataBlockStream = blockArrayCodec.decode(datasetAttributes, blockGridPosition, in);

		final DataBlock<T> dataBlock = dataBlockStream.allocateDataBlock();
		final InputStream stream = Codec.decode(in, blockCodecs);
		dataBlock.readData(dataBlockStream.getDataInput(stream));
		stream.close();

		return dataBlock;
	}

	@Override
	public List<DataBlock<T>> getBlocks() {

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

		for (long[] offsetIndex : sortedOffsets) {
			final long offset = offsetIndex[0];
			if (offset < 0)
				continue;

			final long idx = offsetIndex[1];
			GridIterator.indexToPosition(idx, blocksPerShard, blockGridMin, position);

			final long numBytes = index.getNumBytesByBlockIndex((int)idx);
			try (final InputStream in = splitableData.split(offset, numBytes).newInputStream()) {
				final DataBlock<T> block = getBlock(in, position.clone());
				blocks.add(block);
			} catch (final N5Exception.N5NoSuchKeyException e) {
				return blocks;
			} catch (final IOException | UncheckedIOException e) {
				throw new N5IOException("Failed to read block from " + Arrays.toString(position), e);
			}
		}
		return blocks;
	}

	@Override
	public DataBlock<T> getBlock(long... blockGridPosition) {

		final int[] relativePosition = getBlockPosition(blockGridPosition);
		if (relativePosition == null)
			throw new N5IOException("Attempted to read a block from the wrong shard.");

		final ShardIndex idx = getIndex();
		if (!idx.exists(relativePosition))
			return null;

		final long blockOffset = idx.getOffset(relativePosition);
		final long blockSize = idx.getNumBytes(relativePosition);

		final long[] blockPosInImg = getDatasetAttributes().getBlockPositionFromShardPosition(getGridPosition(), blockGridPosition);
		try (final InputStream in = splitableData.split(blockOffset, blockSize).newInputStream()) {
			return getBlock(in, blockPosInImg);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + Arrays.toString(blockGridPosition), e);
		}
	}

	@Override
	public void writeBlock(final DataBlock<T> block) {

		final int[] relativePosition = getBlockPosition(block.getGridPosition());
		if (relativePosition == null)
			throw new N5IOException("Attempted to write block in the wrong shard.");

		final ShardIndex index = getIndex();
		final long blockOffset;

		//TODO Caleb: is it safe to assume we can ALWAYS write at an offset into a value that doesn't exist yet?
		//	Files seem to fill the beginning with 0, not sure how backends handle this.
		//	Otherwise, may need to explicitly write an empty index first.
		if (index.getLocation() == ShardingCodec.IndexLocation.START)
			blockOffset = splitableData.getSize() == 0 ? index.numBytes() : splitableData.getSize();
		else
			blockOffset = splitableData.getSize();

		final long size = Long.MAX_VALUE - blockOffset;
		final SplitableData data = splitableData.split(blockOffset, size); //TODO Caleb: Should ideally remove offset also, but would need it to be absolute

		final long sizeWritten;
		try (final OutputStream channelOut = data.newOutputStream()) {
			try (final CountingOutputStream out = new CountingOutputStream(channelOut)) {
				writeBlock(out, datasetAttributes, block);

				/* Update and write the index to the shard*/
				sizeWritten = out.getNumBytes();
				index.set(blockOffset, sizeWritten, relativePosition);
			}
		} catch (IOException e) {
			throw new N5IOException("Failed to write block to shard ", e);
		}

		final long indexOffset = index.getLocation() == ShardingCodec.IndexLocation.START ? 0 : blockOffset + sizeWritten;

		try {
			final SplitableData newSplitData = splitableData.split(
					indexOffset,
					index.numBytes());
			ShardIndex.write(newSplitData, index);
		} catch (IOException e) {
			throw new N5IOException("Failed to write index to shard ", e);
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
		final ShardIndex.IndexByteBounds bounds = ShardIndex.byteBounds(index, splitableData.getSize());
		ShardIndex.read(splitableData.split(bounds.start, index.numBytes()), index);
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
