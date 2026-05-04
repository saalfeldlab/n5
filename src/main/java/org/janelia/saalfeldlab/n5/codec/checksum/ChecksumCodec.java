package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeDataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link CodecInfo} that appends a checksum to data when encoding and can
 * validate against that checksum when decoding.
 * <p>
 * Checksum codec instances are expected to be thread safe, but {@link Checksum}
 * implementations may not be. As a result, subclasses of this implementation
 * provide a {@link Supplier} for an appropriate Checksum type, a new instance
 * of which is created by {@link #getChecksum()} for each
 * {@link #encode(ReadData)} and {@link #decode(ReadData)} call.
 */
public abstract class ChecksumCodec implements DataCodec, DataCodecInfo, DeterministicSizeDataCodec {

	private static final long serialVersionUID = 3141427377277375077L;

	private int numChecksumBytes;

	private Supplier<Checksum> checksumSupplier;

	public ChecksumCodec(Supplier<Checksum> checksumSupplier, int numChecksumBytes) {

		this.checksumSupplier = checksumSupplier;
		this.numChecksumBytes = numChecksumBytes;
	}

	/**
	 * Returns a new {@link Checksum} instance. 
	 *
	 * @return the checksum 
	 */
	public Checksum getChecksum() {

		return checksumSupplier.get();
	}

	public int numChecksumBytes() {

		return numChecksumBytes;
	}

	private CheckedOutputStream createStream(OutputStream out) {

		final Checksum checksum = getChecksum();
		return new CheckedOutputStream(out, checksum) {
			private boolean closed = false;
			@Override
			public void close() throws IOException {
				if (!closed) {
					writeChecksum(checksum, out);
					closed = true;
					out.close();
				}
			}
		};
	}

	@Override public ReadData encode(ReadData readData) {

		return readData.encode(this::createStream);
	}

	@Override public ReadData decode(ReadData readData) throws N5IOException {

		final ReadData rdm = readData.materialize();
		final long N = rdm.requireLength();

		final ReadData data = rdm.slice(0, N - numChecksumBytes);
		final long calculatedChecksum = computeChecksum(data);

		final ReadData checksumRd = rdm.slice(N - numChecksumBytes, numChecksumBytes);
		final long storedChecksum = readChecksum(checksumRd);

		if( calculatedChecksum != storedChecksum)
			throw new N5Exception(String.format("Calculated checksum (%d) does not match stored checksum (%d).",
					calculatedChecksum, storedChecksum));

		return data;
	}

	@Override
	public long encodedSize(final long size) {

		return size + numChecksumBytes();
	}

	protected long readChecksum(ReadData checksumData) {

		// the computed checksum is a long that can take values in [0, 2^32 - 1]
		// so convert the four bytes to an appropriate long
		ByteBuffer buf = ByteBuffer.allocate(8);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.put(checksumData.allBytes());
		buf.putInt(0);
		buf.rewind();
		return buf.getLong();
	}

	protected long computeChecksum(ReadData data) {
		final Checksum checksum = getChecksum();
		checksum.update(data.allBytes(), 0, (int)data.requireLength());
		return checksum.getValue();
	}

	/**
	 * Return the value of the checksum as a {@link ByteBuffer} to be serialized.
	 *
	 * @return a ByteBuffer representing the checksum value
	 */
	public abstract ByteBuffer getChecksumValue(Checksum checksum);

	protected void writeChecksum(Checksum checksum, OutputStream out) throws IOException {

		out.write(getChecksumValue(checksum).array());
	}

}
