/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.codec.checksum;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeDataCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link CodecInfo} that appends a checksum to data when encoding and can validate against that checksum when decoding.
 */
public abstract class ChecksumCodec implements DataCodec, DataCodecInfo, DeterministicSizeDataCodec {

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

	private CheckedOutputStream createStream(OutputStream out) {
		return new CheckedOutputStream(out, getChecksum()) {

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

	@Override public ReadData encode(ReadData readData) {

		return readData.encode(this::createStream);

	}

	@Override public ReadData decode(ReadData readData) throws N5IOException {

		return ReadData.from(new CheckedInputStream(readData.inputStream(), getChecksum()));
	}

	@Override
	public long encodedSize(final long size) {

		return size + numChecksumBytes();
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
