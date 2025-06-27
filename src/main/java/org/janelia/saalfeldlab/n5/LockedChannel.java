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

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import java.io.Closeable;
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

	/**
	 * Create a UTF-8 {@link Reader}.
	 *
	 * @return the reader
	 * @throws N5IOException
	 *             if the reader could not be created
	 */
	Reader newReader() throws N5IOException;

	/**
	 * Create a new {@link InputStream}.
	 *
	 * @return the input stream
	 * @throws N5IOException
	 *             if an input stream could not be created
	 */
	InputStream newInputStream() throws N5IOException;

	/**
	 * Create a new UTF-8 {@link Writer}.
	 *
	 * @return the writer
	 * @throws N5IOException
	 *             if a writer could not be created
	 */
	Writer newWriter() throws N5IOException;

	/**
	 * Create a new {@link OutputStream}.
	 *
	 * @return the output stream
	 * @throws N5IOException
	 *             if an output stream could not be created
	 */
	OutputStream newOutputStream() throws N5IOException;
}
