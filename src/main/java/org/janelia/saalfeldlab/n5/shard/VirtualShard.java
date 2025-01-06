package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

public class VirtualShard<T, A extends DatasetAttributes & ShardParameters> extends AbstractShard<T,A> {

	final private KeyValueAccess keyValueAccess;
	final private String path;

	public VirtualShard(final A datasetAttributes, long[] gridPosition,
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

		if (startByte == Shard.EMPTY_INDEX_NBYTES )
			return null;

		final long size = idx.getNumBytes(relativePosition);
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path, startByte, size)) {
			try ( final InputStream channelIn = lockedChannel.newInputStream()) {
				return (DataBlock<T>)DefaultBlockReader.readBlock(channelIn, datasetAttributes, blockGridPosition);
			}
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
				try (final CountingOutputStream out = new CountingOutputStream(channelOut)) {
					DefaultBlockWriter.writeBlock(out, datasetAttributes, block);

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

	public ShardIndex createIndex() {

		// Empty index of the correct size
		return datasetAttributes.createIndex();
	}

	@Override
	public ShardIndex getIndex() {

		try {
			final ShardIndex readIndex = ShardIndex.read(keyValueAccess, path, datasetAttributes.createIndex());
			index = readIndex == null ? createIndex() : readIndex;
		} catch (final N5Exception.N5NoSuchKeyException e) {
			index = createIndex();
		} catch (IOException e) {
			throw new N5IOException("Failed to read index at " + path, e);
		}
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
