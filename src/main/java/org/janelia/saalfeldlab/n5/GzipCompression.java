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
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;

@CompressionType("gzip")
public class GzipCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {

	private static final long serialVersionUID = 8630847239813334263L;

	@CompressionParameter
	private final int level;

	@CompressionParameter
	private final boolean useZlib;

	private final transient GzipParameters parameters = new GzipParameters();

	public GzipCompression() {

		this(Deflater.DEFAULT_COMPRESSION);
	}

	public GzipCompression(final int level) {

		this(level, false);
	}

	public GzipCompression(final int level, final boolean useZlib) {

		this.level = level;
		this.useZlib = useZlib;
	}

	@Override
	public InputStream getInputStream(final InputStream in) throws IOException {

		if (useZlib) {
			return new InflaterInputStream(in);
		} else {
			return new GzipCompressorInputStream(in);
		}
	}

	@Override
	public OutputStream getOutputStream(final OutputStream out) throws IOException {

		if (useZlib) {
			return new DeflaterOutputStream(out, new Deflater(level));
		} else {
			parameters.setCompressionLevel(level);
			return new GzipCompressorOutputStream(out, parameters);
		}
	}

	@Override
	public GzipCompression getReader() {

		return this;
	}

	@Override
	public GzipCompression getWriter() {

		return this;
	}

	private void readObject(final ObjectInputStream in) throws Exception {

		in.defaultReadObject();

		final Field modifiersField = Field.class.getDeclaredField("modifiers");
		final boolean isModifiersAccessible = modifiersField.isAccessible();
		modifiersField.setAccessible(true);
		final Field parametersField = getClass().getDeclaredField("parameters");
		final boolean isFieldAccessible = parametersField.isAccessible();
		parametersField.setAccessible(true);
		final int modifiers = parametersField.getModifiers();
		modifiersField.setInt(parametersField, modifiers & ~Modifier.FINAL);
		parametersField.set(this, new GzipParameters());
		modifiersField.setInt(parametersField, modifiers);
		parametersField.setAccessible(isFieldAccessible);
		modifiersField.setAccessible(isModifiersAccessible);
	}
}
