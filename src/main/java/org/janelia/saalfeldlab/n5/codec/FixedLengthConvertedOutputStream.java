package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

/*
 * An {@link OutputStream} that converts between two fixed-length types.
 */
public class FixedLengthConvertedOutputStream extends OutputStream {

	private final int numBytes;

	private final byte[] raw;
	private final byte[] encoded;

	private final ByteBuffer rawBuffer;
	private final ByteBuffer encodedBuffer;

	private final OutputStream src;

	private BiConsumer<ByteBuffer, ByteBuffer> converter;

	private int incrememntalBytesWritten;

	public FixedLengthConvertedOutputStream(
			final int numBytes,
			final int numBytesAfterEncoding,
			final BiConsumer<ByteBuffer, ByteBuffer> converter,
			final OutputStream src ) {

		this.numBytes = numBytes;
		this.converter = converter;

		raw = new byte[numBytes];
		encoded = new byte[numBytesAfterEncoding];

		rawBuffer = ByteBuffer.wrap(raw);
		encodedBuffer = ByteBuffer.wrap(encoded);

		incrememntalBytesWritten = 0;

		this.src = src;
	}

	@Override
	public void write(int b) throws IOException {

		raw[incrememntalBytesWritten++] = (byte)b;

		// write out the encoded bytes after writing numBytes bytes
		if (incrememntalBytesWritten == numBytes) {

			rawBuffer.rewind();
			encodedBuffer.rewind();

			converter.accept(rawBuffer, encodedBuffer);
			src.write(encoded);
			incrememntalBytesWritten = 0;
		}
	}

}
