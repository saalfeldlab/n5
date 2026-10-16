package org.janelia.saalfeldlab.n5;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.http.ListResponseParser;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class HttpKeyValueRoot implements KeyValueRoot {

	private static final String HEAD = "HEAD";
	private static final String GET = "GET";

	private int readTimeoutMilliseconds = 5000;
	private int connectionTimeoutMilliseconds = 5000;
	private ListResponseParser listDirectoryResponseParser = ListResponseParser.defaultDirectoryListParser();

	private final URI root;

	public HttpKeyValueRoot(final URI root) {
		// The given root URI may not end in a slash in which case we append one.
		final String uriStr = root.toString();
		this.root = uriStr.endsWith("/") ? root : URI.create(uriStr + "/");
	}

	@Deprecated
	@Override
	public KeyValueAccess getKVA() {
		return kva;
	}
	private final KeyValueAccess kva = new HttpKeyValueAccess();

	@Override
	public URI uri() {
		return root;
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) throws N5IOException {
		return VolatileReadData.from(new HttpLazyRead(normalPath));
	}

	@Override
	public boolean isDirectory(final N5Path normalPath) {

		try {
			final URI uri = root.resolve(normalPath.asDirectory().uri());
			final HttpURLConnection http = requireValidHttpResponse(uri, HEAD, false);
			final int code = http.getResponseCode();
			if (code >= 300) {
				final String redirectLocation = http.getHeaderField("Location");
				return locationIsDirectory(redirectLocation);
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public boolean isFile(final N5Path normalPath) {

		try {
			final URI uri = root.resolve(normalPath.asFile().uri());
			final HttpURLConnection http = requireValidHttpResponse(uri, HEAD, false);
			final int code = http.getResponseCode();
			if (code >= 300) {
				final String redirectLocation = http.getHeaderField("Location");
				return !locationIsDirectory(redirectLocation);
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	private static boolean locationIsDirectory(final String redirectLocation) {
		return redirectLocation.endsWith("/") || redirectLocation.endsWith("index.html");
	}

	@Override
	public boolean exists(final N5Path normalPath) {

		try {
			requireValidHttpResponse(root.resolve(normalPath.uri()), HEAD, true);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public long size(final N5FilePath normalPath) throws N5IOException {

		try {
			final HttpURLConnection head = requireValidHttpResponse(root.resolve(normalPath.uri()), HEAD, true);
			return head.getContentLengthLong();
		} catch (FileNotFoundException e) {
			throw new N5NoSuchKeyException("Error getting size: " + normalPath, e);
		} catch (IOException e) {
			throw new N5IOException("Error getting size: " + normalPath, e);
		}
	}

	/**
	 * List all 'directory'-like children of a path.
	 * <p>
	 * Will throw an N5IOException both if a connection to the server can not be
	 * established, or the server does not allow listing.
	 *
	 * @param normalPath
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 *
	 * @return the directories
	 *
	 * @throws N5IOException
	 * 		if an error occurs during listing
	 */
	@Override
	public String[] listDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		try {
			final HttpURLConnection http = requireValidHttpResponse(root.resolve(normalPath.uri()), GET, true);
			final String listResponse = responseToString(http.getInputStream());
			return listDirectoryResponseParser.parseListResponse(listResponse);
		} catch (FileNotFoundException e) {
			throw new N5NoSuchKeyException("Error listing directory at " + normalPath, e);
		} catch (IOException e) {
			throw new N5IOException("Error listing directory at " + normalPath, e);
		}
	}

	private String responseToString(InputStream inputStream) throws IOException {
		return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
	}

	@Override
	public void write(final N5FilePath normalPath, final ReadData data) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}

	@Override
	public void createDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}

	@Override
	public void delete(final N5Path normalPath) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}

	public void setReadTimeout(int readTimeoutMilliseconds) {
		this.readTimeoutMilliseconds = readTimeoutMilliseconds;
	}

	public void setConnectionTimeout(int connectionTimeoutMilliseconds) {
		this.connectionTimeoutMilliseconds = connectionTimeoutMilliseconds;
	}

	public void setListDirectoryParser(final ListResponseParser parser) {
		listDirectoryResponseParser = parser;
	}

	private HttpURLConnection requireValidHttpResponse(
			final URI uri,
			final String method,
			final boolean followRedirects) throws IOException {

		final HttpURLConnection http = (HttpURLConnection) uri.toURL().openConnection();
		http.setReadTimeout(readTimeoutMilliseconds);
		http.setConnectTimeout(connectionTimeoutMilliseconds);
		http.setRequestMethod(method);
		http.setInstanceFollowRedirects(followRedirects);

		final int code = http.getResponseCode();
		if (code < 200 || code >= 400) {
			final String cause = http.getResponseMessage() + " (" + code + ")";
			if (code == 404 || code == 410)
				// NB: We throw FileNotFoundException instead of
				// NoSuchFileException, to be consistent with
				// HttpURLConnection.inputStream(), which also throws
				// FileNotFoundException on 404 or 410.
				throw new FileNotFoundException(cause);
			else
				throw new IOException(cause);
		}

		return http;
	}

	private class HttpLazyRead implements LazyRead {

		private static final String RANGE = "Range";
		private static final String ACCEPT_RANGE = "Accept-Range";
		private static final String BYTES = "bytes";

		private final N5FilePath normalPath;

		HttpLazyRead(final N5FilePath normalPath) {
			this.normalPath = normalPath;
		}

		@Override
		public long size() {
			return HttpKeyValueRoot.this.size(normalPath);
		}

		@Override
		public ReadData materialize(long offset, long length) {

			final URI uri = root.resolve(normalPath.uri());
			try {
				HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
				final boolean isPartialRead = offset > 0 || (length >= 0 && length != Long.MAX_VALUE);
				if (isPartialRead) {
					conn.setRequestProperty(RANGE, rangeString(offset, length));
					final String acceptRanges = conn.getHeaderField(ACCEPT_RANGE);
					if (acceptRanges == null || !acceptRanges.equals(BYTES)) {
						conn.disconnect();
						conn = (HttpURLConnection) uri.toURL().openConnection();
						try(final InputStream in = conn.getInputStream()) {
							return ReadData.from(in).materialize().slice(offset, length);
						}
					}
				}
				try(final InputStream in = conn.getInputStream()) {
					return ReadData.from(in).materialize();
				}
			} catch (FileNotFoundException e) {
				throw new N5NoSuchKeyException("Could not open stream for " + uri, e);
			} catch (IOException e) {
				throw new N5IOException("Could not open stream for " + uri, e);
			}
		}

		private String rangeString(final long startByte, final long size) {

			final String lastByte = (size > 0) ? Long.toString(startByte + size - 1) : "";
			return String.format("%s=%d-%s", BYTES, startByte, lastByte);
		}

		@Override
		public void close() {
		}
	}

}
