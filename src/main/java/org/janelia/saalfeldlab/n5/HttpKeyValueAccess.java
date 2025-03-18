/**
 * Copyright (c) 2017, Stephan Saalfeld All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer. 2. Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * A read-only, non-listable {@link KeyValueAccess} implementation using Http.
 * <p>
 * Attempting to call lockForWriting, createDirectories, or delete will throw an {@link N5Exception}.
 * <p>
 * Attempting to call list, or listDirectories, will throw an {@link N5Exception}. Calling isDirectory always returns false.
 * <p>
 * This was adapted from
 * <a href="https://github.com/scalableminds/n5-http/blob/6c3de37120d65466720a61e1b05cfa87ee3da7c0/src/main/java/com/scalableminds/n5/http/HttpKeyValueAccess.java">work by Norman Rzepka.</a>
 */
public class HttpKeyValueAccess implements KeyValueAccess {

	private int readTimeoutMilliseconds;
	private int connectionTimeoutMilliseconds;

	/**
	 * Opens an {@link HttpKeyValueAccess}
	 *
	 * @throws N5Exception.N5IOException
	 *             if the access could not be created
	 */
	public HttpKeyValueAccess() {

		readTimeoutMilliseconds = 5000;
		connectionTimeoutMilliseconds = 5000;
	}

	public void setReadTimeout(int readTimeoutMilliseconds) {

		this.readTimeoutMilliseconds = readTimeoutMilliseconds;
	}

	public void setConnectionTimeout(int connectionTimeoutMilliseconds) {

		this.connectionTimeoutMilliseconds = connectionTimeoutMilliseconds;
	}

	@Override
	public String[] components(final String path) {

		return Arrays.stream(path.split("/"))
				.filter(x -> !x.isEmpty())
				.toArray(String[]::new);
	}

	@Override
	public String compose(final String... components) {

		return normalize(
				Arrays.stream(components)
						.filter(x -> !x.isEmpty())
						.collect(Collectors.joining("/")));

	}

	/**
	 * Compose a path from a base uri and subsequent components.
	 *
	 * @param uri
	 *            the base path uri
	 * @param components
	 *            the path components
	 * @return the path
	 */
	@Override
	public String compose(final URI uri, final String... components) {

		final String[] uriComponents = new String[components.length + 1];
		System.arraycopy(components, 0, uriComponents, 1, components.length);
		uriComponents[0] = uri.getPath();
		try {
			return new URI(uri.getScheme(), uri.getAuthority(), 
					compose(uriComponents),
					uri.getQuery(), uri.getFragment()).toString();
		} catch (URISyntaxException e) {
			throw new N5Exception(e);
		}
	}

	@Override
	public String parent(final String path) {

		final String[] components = components(path);
		final String[] parentComponents = Arrays.copyOf(components, components.length - 1);

		return compose(parentComponents);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/*
			 * Must pass absolute path to `uri`. if it already is, this is
			 * redundant, and has no impact on the result. It's not true that
			 * the inputs are always referencing absolute paths, but it doesn't
			 * matter in this case, since we only care about the relative
			 * portion of `path` to `base`, so the result always ignores the
			 * absolute prefix anyway.
			 */
			return normalize(uri("/" + base).relativize(uri("/" + path)).toString());
		} catch (final URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
		}
	}

	@Override
	public String normalize(final String path) {

		// TODO fix
		return path;
//		return N5URI.normalizeGroupPath(path);
	}

	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		return new URI(normalPath);
	}

	/**
	 * Test whether the {@code normalPath} exists.
	 * <p>
	 * Removes leading slash from {@code normalPath}, and then checks whether
	 * either {@code path} or {@code path + "/"} is a key.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further efforts are
	 *            made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		try {
			final URL url = uri(normalPath).toURL();
			final HttpURLConnection connection = (HttpURLConnection)url.openConnection();
			connection.setReadTimeout(readTimeoutMilliseconds);
			connection.setConnectTimeout(connectionTimeoutMilliseconds);
			connection.setRequestMethod("HEAD");
			final int code = connection.getResponseCode();
			return (code >= 200 && code < 400); // 2xx (OK) and 3xx (Redirect) are valid responses
		} catch (IOException e) {
			throw new N5Exception("Connection error" , e);
		} catch (URISyntaxException e) {
			throw new N5Exception("Malformed URI" , e);
		}
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none, removes
	 * leading "/", and then checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further efforts are
	 *            made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key,
	 *         {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		// TODO what to do here?
		return false;
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further efforts are
	 *            made to normalize it.
	 * @return {@code true} if {@code path} exists as a key and has no trailing
	 *         slash, {@code false} otherwise
	 */
	@Override
	public boolean isFile(final String normalPath) {

		return exists(normalPath);
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) throws IOException {

		try {
			return new HttpObjectChannel(uri(normalPath));
		} catch (URISyntaxException e) {
			throw new N5Exception("Invalid URI Syntax", e);
		}
	}

	@Override
	public LockedChannel lockForWriting(final String normalPath) throws IOException {

		throw new N5Exception("HttpKeyValueAccess is read-only");
	}

	@Override
	public String[] listDirectories(final String normalPath) {

		throw new N5Exception("HttpKeyValueAccess does not support listing");
	}

	@Override
	public String[] list(final String normalPath) throws IOException {

		throw new N5Exception("HttpKeyValueAccess does not support listing");
	}

	@Override
	public void createDirectories(final String normalPath) {

		throw new N5Exception("HttpKeyValueAccess is read-only");
	}

	@Override
	public void delete(final String normalPath) {

		throw new N5Exception("HttpKeyValueAccess is read-only");
	}

	private class HttpObjectChannel implements LockedChannel {

		protected final URI uri;
		private final ArrayList<Closeable> resources = new ArrayList<>();

		protected HttpObjectChannel(final URI uri) {

			this.uri = uri;
		}

		@Override
		public InputStream newInputStream() throws IOException {

			return uri.toURL().openStream();
		}

		@Override
		public Reader newReader() throws IOException {

			final InputStreamReader reader = new InputStreamReader(newInputStream(), StandardCharsets.UTF_8);
			synchronized (resources) {
				resources.add(reader);
			}
			return reader;
		}

		@Override
		public OutputStream newOutputStream() {

			throw new NonWritableChannelException();
		}

		@Override
		public Writer newWriter() {

			throw new NonWritableChannelException();
		}

		@Override
		public void close() throws IOException {

			synchronized (resources) {
				for (final Closeable resource : resources) {
					resource.close();
				}
				resources.clear();
			}
		}
	}
}
