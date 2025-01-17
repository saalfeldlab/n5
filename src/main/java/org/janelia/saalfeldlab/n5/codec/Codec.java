package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Interface representing a filter can encode a {@link OutputStream}s when writing data, and decode
 * the {@link InputStream}s when reading data.
 *
 * Modeled after <a href="https://zarr.readthedocs.io/en/v2.0.1/api/codecs.html">Filters</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface Codec extends Serializable {

	static OutputStream encode(OutputStream out, Codec.BytesCodec... bytesCodecs) throws IOException {
		OutputStream stream = out;
		for (final BytesCodec codec : bytesCodecs)
			stream = codec.encode(stream);

		return stream;
	}

	static InputStream decode(InputStream out, Codec.BytesCodec... bytesCodecs) throws IOException {
		InputStream stream = out;
		for (final BytesCodec codec : bytesCodecs)
			stream = codec.decode(stream);

		return stream;
	}

	interface BytesCodec extends Codec {

		/**
		 * Decode an {@link InputStream}.
		 *
		 * @param in
		 *            input stream
		 * @return the decoded input stream
		 */
		InputStream decode(final InputStream in) throws IOException;

		/**
		 * Encode an {@link OutputStream}.
		 *
		 * @param out
		 *            the output stream
		 * @return the encoded output stream
		 */
		OutputStream encode(final OutputStream out) throws IOException;
	}

	interface ArrayCodec extends DeterministicSizeCodec {

		default long[] getPositionForBlock(final DatasetAttributes attributes, final DataBlock<?> datablock) {
			return datablock.getGridPosition();
		}

		default long[] getPositionForBlock(final DatasetAttributes attributes, final long... blockPosition) {
			return blockPosition;
		}

		/**
		 * Decode an {@link InputStream}.
		 *
		 * @param in
		 *            input stream
		 * @return the DataBlock corresponding to the input stream
		 */
		DataBlockInputStream decode(
				final DatasetAttributes attributes,
				final long[] gridPosition,
				final InputStream in) throws IOException;

		/**
		 * Encode a {@link DataBlock}.
		 *
		 * @param datablock the datablock to encode
		 */
		DataBlockOutputStream encode(
				final DatasetAttributes attributes,
				final DataBlock<?> datablock,
				final OutputStream out) throws IOException;

		@Override default long encodedSize(long size) {

			return size;
		}

		@Override default long decodedSize(long size) {

			return size;
		}

		default <T> void writeBlock(
				final KeyValueAccess kva,
				final String keyPath,
				final DatasetAttributes datasetAttributes,
				final DataBlock<T> dataBlock) {

			try (final LockedChannel lock = kva.lockForWriting(keyPath)) {
				try (final OutputStream out = lock.newOutputStream()) {
					final DataBlockOutputStream dataBlockOutput = encode(datasetAttributes, dataBlock, out);
					try (final OutputStream stream = Codec.encode(dataBlockOutput, datasetAttributes.getCodecs())) {
						dataBlock.writeData(dataBlockOutput.getDataOutput(stream));
					}
				}
			} catch (final IOException | UncheckedIOException e) {
				final String msg = "Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset " + keyPath;
				throw new N5Exception.N5IOException( msg, e);
			}

		}

		default <T> DataBlock<T> readBlock(
				final KeyValueAccess kva,
				final String keyPath,
				final DatasetAttributes datasetAttributes,
				final long[] gridPosition) {

			try (final LockedChannel lockedChannel = kva.lockForReading(keyPath)) {
				try(final InputStream in = lockedChannel.newInputStream()) {

					final BytesCodec[] codecs = datasetAttributes.getCodecs();
					final ArrayCodec arrayCodec = datasetAttributes.getArrayCodec();
					final DataBlockInputStream dataBlockStream = arrayCodec.decode(datasetAttributes, gridPosition, in);
					InputStream stream = Codec.decode(dataBlockStream, codecs);

					final DataBlock<T> dataBlock = dataBlockStream.allocateDataBlock();
					dataBlock.readData(dataBlockStream.getDataInput(stream));
					stream.close();

					return dataBlock;
				}
			} catch (final N5Exception.N5NoSuchKeyException e) {
				return null;
			} catch (final IOException | UncheckedIOException e) {
				final String msg = "Failed to read block " + Arrays.toString(gridPosition) + " from dataset " + keyPath;
				throw new N5Exception.N5IOException( msg, e);
			}
		}
	}

	abstract class DataBlockInputStream extends ProxyInputStream {


		protected DataBlockInputStream(InputStream in) {

			super(in);
		}

		public abstract <T> DataBlock<T> allocateDataBlock() throws IOException;

		public abstract DataInput getDataInput(final InputStream inputStream);
	}

	abstract class DataBlockOutputStream extends ProxyOutputStream {

		protected DataBlockOutputStream(final OutputStream out) {

			super(out);
		}

		public abstract DataOutput getDataOutput(final OutputStream outputStream);
	}

	String getType();
}

