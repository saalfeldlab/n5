package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public class ChecksumInputStream extends InputStream {

	private final Checksum checksum;
	// private final long expected;
	private InputStream in;

	public ChecksumInputStream(Checksum checksum, InputStream in) {

		// this needs to know how many bytes are in its checksum
		// Maybe pass the codec here instead of the checksum
		this.checksum = checksum;
		this.in = in;
	}

	@Override
	public int read() throws IOException {

		// returns -1 if end of the stream is reached
		final int b = in.read();
		checksum.update(b);
		return b;
	}

	protected long readChecksum() throws IOException {

		final byte[] checksum = new byte[getChecksumSize()];
		in.read(checksum);
		return ByteBuffer.wrap(checksum).getLong();
	}

	public boolean validate() throws IOException {

		// TODO consider reading N more bytes (the checksum)
		// set expected to that, then validate
		return readChecksum() == checksum.getValue();
	}

	public int getChecksumSize() {

		return -1;
	}

}
