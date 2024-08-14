package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public class VirtualShard<T> extends AbstractShard<T> {

	private KeyValueAccess keyValueAccess;
	private String path;

	public VirtualShard(final ShardedDatasetAttributes datasetAttributes, long[] gridPosition,
			final KeyValueAccess keyValueAccess, final String path) {

		super(datasetAttributes, gridPosition, null);
		this.keyValueAccess = keyValueAccess;
		this.path = path;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<T> getBlock(long... blockGridPosition) {

		final long[] relativePosition = getBlockPosition(blockGridPosition);
		if (relativePosition == null)
			throw new N5IOException("Attempted to read a block from the wrong shard.");

		final ShardIndex idx = getIndex();
		final long startByte = idx.getOffset(relativePosition);
		final long endByte = startByte + idx.getNumBytes(relativePosition);
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path, startByte, endByte)) {

			// TODO add codecs, generalize to use any BlockReader
			final DataBlock<T> dataBlock = (DataBlock<T>)datasetAttributes.getDataType().createDataBlock(
					datasetAttributes.getBlockSize(),
					blockGridPosition,
					numBlockElements(datasetAttributes));

			DefaultBlockReader.readFromStream(dataBlock, lockedChannel.newInputStream());
			return dataBlock;

		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}
	}

	@Override
	public void writeBlock(final DataBlock<T> block) {

		final long[] relativePosition = getBlockPosition(block.getGridPosition());
		if (relativePosition == null)
			throw new N5IOException("Attempted to write block in the wrong shard.");

		final ShardIndex idx = getIndex();
		final long startByte = idx.getOffset(relativePosition) == Shard.EMPTY_INDEX_NBYTES ? 0 : idx.getOffset(relativePosition);
		final long size = idx.getNumBytes(relativePosition) == Shard.EMPTY_INDEX_NBYTES ? Long.MAX_VALUE : idx.getNumBytes(relativePosition);

		// TODO this assumes that the block exists in the shard and
		// that the available space is sufficient. Should generalize
		try (final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path, startByte, size)) {

			// TODO codecs
			final CountingOutputStream out = new CountingOutputStream(lockedChannel.newOutputStream());
			datasetAttributes.getCompression().getWriter().write(block, out);

			// TODO update index when we know how many bytes were written
			idx.set(startByte, out.getNumBytes(), relativePosition);
			out.write(index.toByteBuffer().array());
			out.realClose();
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}

	}

	@Override
	public void writeShard() {

		// TODO
	}

	private static int numBlockElements(DatasetAttributes datasetAttributes) {

		return Arrays.stream(datasetAttributes.getBlockSize()).reduce(1, (x, y) -> x * y);
	}

	public ShardIndex createIndex() {

		// Empty index of the correct size
		index = new ShardIndex(datasetAttributes.getShardBlockGridSize());
		return index;
	}

	@Override
	public ShardIndex getIndex() {

		try {
			final ShardIndex result = ShardIndex.read(keyValueAccess, path, datasetAttributes);
			return result == null ? createIndex() : result;
		} catch (final NoSuchFileException e) {
			return createIndex();
		} catch (IOException e) {
			throw new N5IOException("Failed to read index at " + path, e);
		}
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

		private void realClose() throws IOException {
			out.close();
		}

		public long getNumBytes() {
			return numBytes;
		}
	}
}
