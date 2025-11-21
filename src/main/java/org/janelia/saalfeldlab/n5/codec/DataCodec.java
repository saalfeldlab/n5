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

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * {@code DataCodec}s transform one {@link ReadData} into another,
 * for example, compressing it.
 */
public interface DataCodec {

	/**
	 * Decode the given {@link ReadData}.
	 * <p>
	 * The returned decoded {@code ReadData} reports {@link ReadData#length()
	 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
	 * depending on the {@code DataCodec} implementation.
	 *
	 * @param readData
	 * 		data to decode
	 *
	 * @return decoded ReadData
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData decode(ReadData readData) throws N5IOException;

	/**
	 * Encode the given {@link ReadData}.
	 * <p>
	 * Encoding may be lazy or eager, depending on the {@code DataCodec}
	 * implementation.
	 *
	 * @param readData
	 * 		data to encode
	 *
	 * @return encoded ReadData
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData encode(ReadData readData) throws N5IOException;

	/**
	 * Create a {@code DataCodec} that sequentially applies {@code codecs} in
	 * the given order for encoding, and in reverse order for decoding.
	 * <p>
	 * If all {@code codecs} implement {@code DeterministicSizeDataCodec}, the
	 * returned {@code DataCodec} will also be a {@code DeterministicSizeDataCodec}.
	 *
	 * @param codecs
	 *            a list of DataCodecs
	 * @return the concatenated DataCodec
	 */
	static DataCodec concatenate(final DataCodec... codecs) {

		if (codecs == null)
			throw new NullPointerException();

		if (codecs.length == 1)
			return codecs[0];

		if (Arrays.stream(codecs).allMatch(DeterministicSizeDataCodec.class::isInstance))
			return new ConcatenatedDeterministicSizeDataCodec(Arrays.copyOf(codecs, codecs.length, DeterministicSizeDataCodec[].class));
		else
			return new ConcatenatedDataCodec(codecs);
	}

	static DataCodec create(final DataCodecInfo... codecInfos) {

		if (codecInfos == null)
			throw new NullPointerException();

		final DataCodec[] codecs = new DataCodec[codecInfos.length];
		Arrays.setAll(codecs, i -> codecInfos[i].create());

		return DataCodec.concatenate(codecs);
	}

}
