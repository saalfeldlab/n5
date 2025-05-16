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

import java.nio.file.FileSystems;

import com.google.gson.GsonBuilder;

/**
 * Filesystem {@link N5Writer} implementation with version compatibility check.
 *
 * @author Stephan Saalfeld
 */
public class N5FSWriter extends N5KeyValueWriter {

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath
	 *            n5 base path
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheAttributes
	 *            cache attributes and meta data
	 *            Setting this to true avoidsfrequent reading and parsing of
	 *            JSON encoded attributes andother meta data that requires
	 *            accessing the store. This ismost interesting for high latency
	 *            backends. Changes of cachedattributes and meta data by an
	 *            independent writer on the samecontainer will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes)
			throws N5Exception {

		super(
				new FileSystemKeyValueAccess(FileSystems.getDefault()),
				basePath,
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 *
	 * If the base path does not exist, it will be created.
	 *
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 *
	 * @param basePath
	 *            base path
	 * @param cacheAttributes
	 *            attributes and meta data
	 *            Setting this to true avoidsfrequent reading and parsing of
	 *            JSON encoded attributes andother meta data that requires
	 *            accessing the store. This ismost interesting for high latency
	 *            backends. Changes of cachedattributes and meta data by an
	 *            independent writer on the samecontainer will not be tracked.
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final boolean cacheAttributes) throws N5Exception {

		this(basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path with a custom
	 * {@link GsonBuilder} to support custom attributes.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath
	 *            base path
	 * @param gsonBuilder
	 *            gson builder
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5FSWriter} at a given base path.
	 * <p>
	 * If the base path does not exist, it will be created.
	 * </p>
	 * <p>
	 * If the base path exists and if the N5 version of the container is
	 * compatible with this implementation, the N5 version of this container
	 * will be set to the current N5 version of this implementation.
	 * </p>
	 *
	 * @param basePath
	 *            n5 base path
	 *
	 * @throws N5Exception
	 *             if the base path cannot be written to or cannot be created,
	 *             if the N5 version of the container is not compatible with
	 *             this implementation.
	 */
	public N5FSWriter(final String basePath) throws N5Exception {

		this(basePath, new GsonBuilder());
	}
}
