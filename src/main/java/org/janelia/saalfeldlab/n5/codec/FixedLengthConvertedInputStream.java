package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

/**
 * An {@link InputStream} that converts between two fixed-length types.
 */
public class FixedLengthConvertedInputStream extends InputStream {

	private final int numBytes;
	private final int numBytesAfterDecoding;

	private final byte[] raw;
	private final byte[] decoded;

	private final ByteBuffer rawBuffer;
	private final ByteBuffer decodedBuffer;

	private final InputStream src;

	private BiConsumer<ByteBuffer, ByteBuffer> converter;

	private int incrememntalBytesRead;

	public FixedLengthConvertedInputStream(
			final int numBytes,
			final int numBytesAfterDecoding,
			BiConsumer<ByteBuffer, ByteBuffer> converter,
			final InputStream src ) {

		this.numBytes = numBytes;
		this.numBytesAfterDecoding = numBytesAfterDecoding;
		this.converter = converter;

		raw = new byte[numBytes];
		decoded = new byte[numBytesAfterDecoding];
		incrememntalBytesRead = 0;

		rawBuffer = ByteBuffer.wrap(raw);
		decodedBuffer = ByteBuffer.wrap(decoded);

		this.src = src;
	}

	@Override
	public int read() throws IOException {

		// TODO not sure if this always reads enough bytes
		// int n = src.read(toEncode);
		if (incrememntalBytesRead == 0) {

			rawBuffer.rewind();
			decodedBuffer.rewind();

			for (int i = 0; i < numBytes; i++) {
				final int retval = src.read();
				if (retval == -1 && i == 0)
					return retval;
				else if (retval == -1)
					throw new IOException("Unexpected end of stream");

				raw[i] = (byte)retval;
			}

			converter.accept(rawBuffer, decodedBuffer);
		}

		final int out = decoded[incrememntalBytesRead++];
		if (incrememntalBytesRead == numBytesAfterDecoding)
			incrememntalBytesRead = 0;

		return out;
	}

}
