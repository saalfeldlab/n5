package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link Codec} that appends a checksum to data when encoding and can validate against that checksum when decoding.
 */
public abstract class ChecksumCodec implements BytesCodec, DeterministicSizeCodec {

	private static final long serialVersionUID = 3141427377277375077L;

	private int numChecksumBytes;

	private Checksum checksum;

	public ChecksumCodec(Checksum checksum, int numChecksumBytes) {

		this.checksum = checksum;
		this.numChecksumBytes = numChecksumBytes;
	}

	public Checksum getChecksum() {

		return checksum;
	}

	public int numChecksumBytes() {

		return numChecksumBytes;
	}

	private CheckedOutputStream createStream(OutputStream out) throws IOException {
		return  new CheckedOutputStream(out, getChecksum()) {

			private boolean closed = false;
			@Override public void close() throws IOException {

				if (!closed) {
					writeChecksum(out);
					closed = true;
					out.close();
				}
			}
		};
	}

	@Override public ReadData encode(ReadData readData) throws IOException {

		return readData.encode(this::createStream);

	}

	@Override public ReadData decode(ReadData readData) throws IOException {


		return ReadData.from(new CheckedInputStream(readData.inputStream(), getChecksum()));
	}

	@Override
	public long encodedSize(final long size) {

		return size + numChecksumBytes();
	}

	@Override
	public long decodedSize(final long size) {

		return size - numChecksumBytes();
	}

	protected boolean valid(InputStream in) throws IOException {

		return readChecksum(in) == getChecksum().getValue();
	}

	protected long readChecksum(InputStream in) throws IOException {

		final byte[] checksum = new byte[numChecksumBytes()];
		in.read(checksum);
		return ByteBuffer.wrap(checksum).getLong();
	}

	/**
	 * Return the value of the checksum as a {@link ByteBuffer} to be serialized.
	 *
	 * @return a ByteBuffer representing the checksum value
	 */
	public abstract ByteBuffer getChecksumValue();

	public void writeChecksum(OutputStream out) throws IOException {

		out.write(getChecksumValue().array());
	}


}
