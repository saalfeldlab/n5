package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Checksum;

import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;

/**
 * A {@link Codec} that appends a checksum to data when encoding and can validate against that checksum when decoding.
 */
public abstract class ChecksumCodec implements DeterministicSizeCodec {

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

	@Override
	public ChecksumInputStream decode(final InputStream in) throws IOException {

		// TODO get the correct expected checksum
		// TODO write a test with nested checksum codecs

		// has to know the number of it needs to read?
		return new ChecksumInputStream(getChecksum(), in);
	}

	@Override
	public ChecksumOutputStream encode(final OutputStream out) throws IOException {

		// when do we validate
		return new ChecksumOutputStream(getChecksum(), out);
	}

	@Override
	public long encodedSize(final long size) {

		return size + numChecksumBytes();
	}

	@Override
	public long decodedSize(final long size) {

		return size - numChecksumBytes();
	}

	public boolean validate() {

		// TODO implement
		// does validate go here or in ChecksumOutputStream
		return true;
	}

}
