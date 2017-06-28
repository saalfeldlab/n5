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
import java.util.HashMap;

import com.google.gson.JsonElement;

/**
 * A simple structured container format for hierarchies of chunked
 * n-dimensional datasets and attributes.
 *
 * {@linkplain https://github.com/axtimwalde/n5}
 *
 * @author Stephan Saalfeld
 */
public interface N5Reader {

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 *
	 * TODO uses file locks to synchronize with other processes, now also
	 *   synchronize for threads inside the JVM
	 */
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException;

	/**
	 * Reads an attribute.
	 *
	 * @param pathName group path
	 * @param key
	 * @param clazz attribute class
	 * @return
	 */
	public <T> T getAttribute(
			final String pathName,
			final String key,
			final Class<T> clazz) throws IOException;

	/**
	 * Get mandatory dataset attributes.
	 *
	 * @param pathName dataset path
	 * @return dataset attributes or null if either dimensions or dataType are not set
	 * @throws IOException
	 */
	public DatasetAttributes getDatasetAttributes(final String pathName) throws IOException;

	/**
	 * Reads a {@link DataBlock}.
	 *
	 * @param pathName dataset path
	 * @param datasetAttributes
	 * @param gridPosition
	 * @return
	 * @throws IOException
	 */
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException;

	/**
	 * Test whether a group or dataset exists.
	 *
	 * @param pathName group path
	 * @return
	 */
	public boolean exists(final String pathName);

	/**
	 * Test whether a dataset exists.
	 *
	 * @param pathName dataset path
	 * @return
	 */
	public boolean datasetExists(final String pathName) throws IOException;

	/**
	 * Test whether a group or dataset has attributes.
	 *
	 * @param pathName group path
	 * @return
	 */
	public boolean hasAttributes(final String pathName);

	/**
	 * List all groups (including datasets) in a group.
	 *
	 * @param pathName group path
	 * @return
	 * @throws IOException
	 */
	public String[] list(final String pathName) throws IOException;
}
