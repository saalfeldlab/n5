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
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.janelia.saalfeldlab.n5.Compression.CompressionType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@CompressionType("gzip")
@NameConfig.Name("gzip")
public class GzipCompression implements DefaultBlockReader, DefaultBlockWriter, Compression {

	private static final long serialVersionUID = 8630847239813334263L;

	@CompressionParameter
	@NameConfig.Parameter
	//TODO Caleb: How to handle serialization of parameter-less constructor.
	// For N5 the default is -1.
	// For zarr the range is 0-9 and is required.
	// How to map -1 to some default (1?) when serializing to zarr?
	private final int level;

	@CompressionParameter
	@NameConfig.Parameter(optional = true)
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

	private InputStream getInputStream(final InputStream in) throws IOException {

		if (useZlib) {
			return new InflaterInputStream(in);
		} else {
			return new GzipCompressorInputStream(in, true);
		}
	}

	private OutputStream getOutputStream(final OutputStream out) throws IOException {

		if (useZlib) {
			return new DeflaterOutputStream(out, new Deflater(level));
		} else {
			parameters.setCompressionLevel(level);
			return new GzipCompressorOutputStream(out, parameters);
		}
	}

	private void readObject(final ObjectInputStream in) throws Exception {

		in.defaultReadObject();
		ReflectionUtils.setFieldValue(this, "parameters", new GzipParameters());
	}

	@Override
	public boolean equals(final Object other) {

		if (other == null || other.getClass() != GzipCompression.class)
			return false;
		else {
			final GzipCompression gz = ((GzipCompression)other);
			return useZlib == gz.useZlib && level == gz.level;
		}
	}

	@Override
	public ReadData decode(final ReadData readData) throws IOException {
		return ReadData.from(getInputStream(readData.inputStream()));
	}

	@Override
	public ReadData encode(final ReadData readData) {
		return readData.encode(this::getOutputStream);
	}

}
