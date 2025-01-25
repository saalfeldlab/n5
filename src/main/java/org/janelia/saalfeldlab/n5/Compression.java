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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
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

	// ---------------------------------------------------------------------------------
	// TODO. clean up interface hierarchy.
	//       getInputStream and getOutputStream are duplicated here from DefaultBlockReader/Writer
	//       to allow for default implementation of decode/encode (which are copied from wip/codecsShards).
	InputStream getInputStream(final InputStream in) throws IOException;

	OutputStream getOutputStream(final OutputStream out) throws IOException;

	/**
	 * Decode an {@link InputStream}.
	 *
	 * @param in
	 *            input stream
	 * @return the decoded input stream
	 */
	default InputStream decode(InputStream in) throws IOException {
		return getInputStream(in);
	}

	/**
	 * Encode an {@link OutputStream}.
	 *
	 * @param out
	 *            the output stream
	 * @return the encoded output stream
	 */
	default OutputStream encode(OutputStream out) throws IOException {
		return getOutputStream(out);
	}

	// TODO probably remove?
	default byte[] encode(byte[] data) throws IOException {
		final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		final OutputStream encodedStream = encode(byteStream);
		encodedStream.write(data);
		encodedStream.close();
		return byteStream.toByteArray();
	}

	// TODO probably remove?
	default byte[] decode(byte[] data) throws IOException {
		return Java9StreamMethods.readAllBytes(decode(new ByteArrayInputStream(data)));
	}
}
