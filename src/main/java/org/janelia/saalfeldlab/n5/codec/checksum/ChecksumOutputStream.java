package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public class ChecksumOutputStream extends OutputStream {

	private final Checksum checksum;
	private OutputStream out;

	public ChecksumOutputStream(Checksum checksum, OutputStream out) {

		this.checksum = checksum;
		this.out = out;
	}

	@Override
	public void write(int b) throws IOException {

		checksum.update(b);
		out.write(b);
	}

	public void finish() throws IOException {

		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.asLongBuffer().put(checksum.getValue());
		out.write(buf.array());
	}

}
