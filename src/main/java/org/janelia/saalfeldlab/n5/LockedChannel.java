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
