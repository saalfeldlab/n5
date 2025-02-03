/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;

import java.io.InputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.janelia.saalfeldlab.n5.readdata.OutputStreamEncoder.EncodedOutputStream;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

@CompressionType("lz4")
public class Lz4Compression implements Compression {

	private static final long serialVersionUID = -9071316415067427256L;

	@CompressionParameter
	private final int blockSize;

	public Lz4Compression(final int blockSize) {

		this.blockSize = blockSize;
	}

	public Lz4Compression() {

		this(1 << 16);
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != Lz4Compression.class)
			return false;
		else
			return blockSize == ((Lz4Compression)other).blockSize;
	}

	@Override
	public ReadData decode(final ReadData readData, final int decodedLength) throws IOException {
		final InputStream inflater = new LZ4BlockInputStream(readData.inputStream());
		return ReadData.from(inflater, decodedLength).order(readData.order());
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(out -> {
			final LZ4BlockOutputStream deflater = new LZ4BlockOutputStream(out, blockSize);
			return new EncodedOutputStream(deflater, deflater::finish);
		});
	}
}
