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
package org.janelia.saalfeldlab.n5.codec;

public class IndexCodecAdapter {

	private final BlockCodecInfo blockCodecInfo;
	private final DeterministicSizeCodecInfo[] dataCodecs;

	public IndexCodecAdapter(final BlockCodecInfo blockCodecInfo, final DeterministicSizeCodecInfo... dataCodecs) {

		this.blockCodecInfo = blockCodecInfo;
		this.dataCodecs = dataCodecs;
	}

	public BlockCodecInfo getBlockCodecInfo() {

		return blockCodecInfo;
	}

	public DataCodecInfo[] getDataCodecs() {

		final DataCodecInfo[] dataCodecs = new DataCodecInfo[this.dataCodecs.length];
		System.arraycopy(this.dataCodecs, 0, dataCodecs, 0, this.dataCodecs.length);
		return dataCodecs;
	}

	public long encodedSize(long initialSize) {
		long totalNumBytes = initialSize;
		for (DeterministicSizeCodecInfo codec : dataCodecs) {
			totalNumBytes = codec.encodedSize(totalNumBytes);
		}
		return totalNumBytes;
	}

	public static IndexCodecAdapter create(final CodecInfo... codecs) {
		if (codecs == null || codecs.length == 0)
			return new IndexCodecAdapter(new RawBlockCodecInfo());

		if (codecs[0] instanceof BlockCodecInfo)
			return new IndexCodecAdapter((BlockCodecInfo)codecs[0]);

		final DeterministicSizeCodecInfo[] indexCodecs = new DeterministicSizeCodecInfo[codecs.length];
		System.arraycopy(codecs, 0, indexCodecs, 0, codecs.length);
		return new IndexCodecAdapter(new RawBlockCodecInfo(), indexCodecs);
	}
}
