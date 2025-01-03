/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * A lock on a path that can create a {@link Reader}, {@link Writer},
 * {@link InputStream}, or {@link OutputStream}.
 *
 * @author Stephan Saalfeld
 */
public interface LockedChannel extends Closeable {

	public long size() throws IOException;

	/**
	 * Create a UTF-8 {@link Reader}.
	 *
	 * @return the reader
	 * @throws IOException
	 *             if the reader could not be created
	 */
	public Reader newReader() throws IOException;

	/**
	 * Create a new {@link InputStream}.
	 *
	 * @return the input stream
	 * @throws IOException
	 *             if an input stream could not be created
	 */
	public InputStream newInputStream() throws IOException;

	/**
	 * Create a new UTF-8 {@link Writer}.
	 *
	 * @return the writer
	 * @throws IOException
	 *             if a writer could not be created
	 */
	public Writer newWriter() throws IOException;

	/**
	 * Create a new {@link OutputStream}.
	 *
	 * @return the output stream
	 * @throws IOException
	 *             if an output stream could not be created
	 */
	public OutputStream newOutputStream() throws IOException;
}
