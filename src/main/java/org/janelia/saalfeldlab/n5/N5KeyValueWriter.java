/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
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

import com.google.gson.GsonBuilder;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5KeyValueWriter extends N5KeyValueReader implements CachedGsonKeyValueN5ShardWriter {

	/**
	 * Opens an {@link N5KeyValueWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * <p>
	 * If the base path does not exist, it will be created.
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param keyValueAccess
	 * 			  the backend key value access to use
	 * @param basePath
	 *            n5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheAttributes
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes, this is most interesting for high
	 *            latency file systems. Changes of attributes by an independent
	 *            writer will not be tracked.
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5KeyValueWriter(
			final KeyValueAccess keyValueAccess,
			final String basePath,
			final GsonBuilder gsonBuilder,
			final boolean cacheAttributes)
			throws N5Exception {

		super(false, keyValueAccess, basePath, gsonBuilder, cacheAttributes, false);

		Version version = null;
		try {
			version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new N5Exception.N5IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		} catch (final NullPointerException e) {}

		if (version == null || version.equals(new Version(0, 0, 0, ""))) {
			createGroup("/");
			setVersion("/");
		}
	}
}
