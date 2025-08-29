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
package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.scijava.annotations.Indexable;

/**
 * This interface is used to indicate that a {@link DataCodec} can be
 * serialized as a "compression" for the N5 format (using the N5 API).
 * <p>
 * N5Readers and N5Writers for the N5 format can declare BytesCodecs that
 * implement this interface so that the {@link CompressionAdapter} is used for
 * serialization.
 * <p>
 * See also: an alternative method for serializing general {@link CodecInfo}s is
 * with the {@link NameConfigAdapter}.
 *
 * @author Stephan Saalfeld
 */
public interface Compression extends Serializable, DataCodec {

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

	@Override
	default String getType() {

		final CompressionType compressionType = getClass().getAnnotation(CompressionType.class);
		if (compressionType == null)
			return null;
		else
			return compressionType.value();
	}
}
