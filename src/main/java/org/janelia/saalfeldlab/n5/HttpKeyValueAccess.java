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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.function.TriFunction;

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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A read-only {@link KeyValueAccess} implementation using HTTP. As a result, calling <code>lockForWriting</code>, <code>createDirectories</code>, or <code>delete</code> will throw an {@link N5Exception}.
 * <p>
 * The behavior of <code>list</code>, <code>listDirectories</code>, and <code>isDirectory</code> will depend on the server configuration. See the documentation of those methods for details.
 * <p>
 * Methods that take a "normalPath" as an argument expect absolute URIs.
 */
public class HttpKeyValueAccess implements KeyValueAccess {

	private static final Pattern LIST_DIR_ENTRY = Pattern.compile("href=\"[^\"]+\">(?<entry>[^<]+)/");
	private static final Pattern LIST_ENTRY = Pattern.compile("href=\"[^\"]+\">(?<entry>[^<]+)");
	private int readTimeoutMilliseconds;
	private int connectionTimeoutMilliseconds;

	/**
	 * Opens an {@link HttpKeyValueAccess}
	 *
	 * @throws N5Exception.N5IOException if the access could not be created
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
	public String normalize(final String path) {

		return N5URI.normalizeGroupPath(path);
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
	 * @param normalPath is expected to be in normalized form, no further efforts are
	 *                   made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		requireValidHttpResponse(normalPath, "HEAD", "Error checking existence: " + normalPath, true);
		return true;
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none, removes
	 * leading "/", and then checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further efforts are
	 *                   made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key,
	 * {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		try {
			requireValidHttpResponse(getDirectoryPath(normalPath), "HEAD", (code,  msg,http) -> {
				final N5Exception cause = validExistsResponse(code, "Error checking directory: " + normalPath, msg, true);
				if (code >= 300 && code < 400) {
					final String redirectLocation = http.getHeaderField("Location");
					if (!(redirectLocation.endsWith("/") || redirectLocation.endsWith("index.html")))
						return new N5Exception.N5NoSuchKeyException("Found File at " + normalPath + " but was not directory");
					return null;
				}
				return cause;
			});
			return true;
		} catch (N5Exception e) {
			return false;
		}
	}

	private static String getDirectoryPath(String normalPath) {

		final String directoryNormalPath;
		if (normalPath.endsWith("/"))
			directoryNormalPath = normalPath;
		else
			directoryNormalPath = normalPath + "/";
		return directoryNormalPath;
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further efforts are
	 *                   made to normalize it.
	 * @return {@code true} if {@code path} exists as a key and has no trailing
	 * slash, {@code false} otherwise
	 */
	@Override
	public boolean isFile(final String normalPath) {

		/* Files must not end in `/` And Don't accept a redirect to a location ending in `/` */
		try {
			requireValidHttpResponse(getFilePath(normalPath), "HEAD", (code, msg, http) -> {
				final N5Exception cause = validExistsResponse(code, "Error accessing file: " + normalPath, msg, true);
				if (code >= 300 && code < 400) {
					final String redirectLocation = http.getHeaderField("Location");
					if (redirectLocation.endsWith("/") || redirectLocation.endsWith("index.html"))
						return new N5Exception.N5NoSuchKeyException("Found key at " + normalPath + " but was directory");
				}
				return cause;
			});
			return true;
		} catch (N5Exception e) {
			return false;
		}
	}

	private static String getFilePath(String normalPath) {

		final String fileNormalPath = normalPath.replaceAll("/+$", "");
		return fileNormalPath;
	}

	private HttpURLConnection httpRequest(String normalPath, String method) throws IOException {

		final URL url = URI.create(normalPath).toURL();
		final HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		connection.setReadTimeout(readTimeoutMilliseconds);
		connection.setConnectTimeout(connectionTimeoutMilliseconds);
		connection.setRequestMethod(method);
		return connection;
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) throws IOException {
		//TODO Caleb: Maybe check exists lazily when attempting to read
		try {
			if (!exists(normalPath))
				throw new N5Exception.N5NoSuchKeyException("Key does not exist: " + normalPath);
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

		return queryListEntries(normalPath, LIST_DIR_ENTRY, true);
	}

	@Override
	public String[] list(final String normalPath) throws IOException {

		return queryListEntries(normalPath, LIST_ENTRY, true);
	}

	private String[] queryListEntries(String normalPath, Pattern listEntry, boolean allowRedirect) {

		final HttpURLConnection http = requireValidHttpResponse(normalPath, "GET", "Error listing directory at " + normalPath, allowRedirect);
		try {
			final String listResponse = responseToString(http.getInputStream());
			final Matcher matcher = listEntry.matcher(listResponse);
			final List<String> matches = new ArrayList<>();
			while (matcher.find()) {
				matches.add(matcher.group("entry"));
			}
			return matches.toArray(new String[0]);
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Error listing directory at " + normalPath, e);
		}
	}

	private static N5Exception validExistsResponse(int code, String responseMsg, String message, boolean allowRedirect) {
		if (code >= 200 && code < (allowRedirect ? 400 : 300)) return null;

		final RuntimeException cause = new RuntimeException(message + "( "+ responseMsg + ")(" + code + ")");
		if (code == 404)
			return new N5Exception.N5NoSuchKeyException(message, cause);

		return new N5Exception(message, cause);
	}

	private HttpURLConnection requireValidHttpResponse(String uri, String method, String message, boolean allowRedirect) throws N5Exception {
		return requireValidHttpResponse(uri, method, (code, msg, http) -> validExistsResponse(code, msg, message, allowRedirect));
	}

	private HttpURLConnection requireValidHttpResponse(String uri, String method, TriFunction<Integer, String, HttpURLConnection, N5Exception> filterCode) throws N5Exception {

		final int code;
		final HttpURLConnection http;
		final String responseMsg;
		try {
			http = httpRequest(uri, method);
			code = http.getResponseCode();
			responseMsg = http.getResponseMessage();
		} catch (IOException e) {
			throw new N5Exception.N5IOException("Could not validate HTTP Response", e);
		}

		final N5Exception cause = filterCode.apply(code, responseMsg, http);
		if (cause != null) throw cause;
		return http;
	}

	private String responseToString(InputStream inputStream) throws IOException {

		return IOUtils.toString(inputStream, StandardCharsets.UTF_8.name());
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
