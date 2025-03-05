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
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

@CompressionType("bzip2")
public class Bzip2Compression implements Compression {

	private static final long serialVersionUID = -4873117458390529118L;

	@CompressionParameter
	private final int blockSize;

	public Bzip2Compression(final int blockSize) {

		this.blockSize = blockSize;
	}

	public Bzip2Compression() {

		this(BZip2CompressorOutputStream.MAX_BLOCKSIZE);
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != Bzip2Compression.class)
			return false;
		else
			return blockSize == ((Bzip2Compression)other).blockSize;
	}

	@Override
	public ReadData decode(final ReadData readData) throws IOException {

		return ReadData.from(new BZip2CompressorInputStream(readData.inputStream()));
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(out -> new BZip2CompressorOutputStream(out, blockSize));
	}
}
