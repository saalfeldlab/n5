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
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.scijava.annotations.Indexable;

/**
 * Compression scheme interface.
 *
 * @author Stephan Saalfeld
 */
public interface Compression extends Serializable {

	/**
	 * Annotation for runtime discovery of compression schemes.
	 *
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable
	@interface CompressionType {

		String value();
	}

	/**
	 * Annotation for runtime discovery of compression schemes.
	 *
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.FIELD)
	@interface CompressionParameter {}

	default String getType() {

		final CompressionType compressionType = getClass().getAnnotation(CompressionType.class);
		if (compressionType == null)
			return null;
		else
			return compressionType.value();
	}

	// --------------------------------------------------
	//

	/**
	 * Decode the given {@code readData}.
	 * <p>
	 * The returned decoded {@code ReadData} reports {@link ReadData#length()
	 * length()}{@code == decodedLength}. Decoding may be lazy or eager,
	 * depending on the {@code BytesCodec} implementation.
	 *
	 * @param readData
	 * 		data to decode
	 * @param decodedLength
	 * 		length of the decoded data (-1 if unknown)
	 *
	 * @return decoded ReadData
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 */
	ReadData decode(ReadData readData, int decodedLength) throws IOException;

	/**
	 * Encode the given {@code readData}.
	 * <p>
	 * Encoding may be lazy or eager, depending on the {@code BytesCodec}
	 * implementation.
	 *
	 * @param readData
	 * 		data to encode
	 *
	 * @return encoded ReadData
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 */
	ReadData encode(ReadData readData) throws IOException;

}
