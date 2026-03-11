package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.function.TriFunction;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.http.ListResponseParser;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class RootedHttpKeyValueAccess implements RootedKeyValueAccess {

	private final URI root;

	public RootedHttpKeyValueAccess(final URI root) {
		// The given root URI may not end in a slash in which case we append one.
		final String uriStr = root.toString();
		this.root = uriStr.endsWith("/") ? root : URI.create(uriStr + "/");
	}

	public RootedHttpKeyValueAccess(final String root) {
		this(URI.create(root));
	}

	@Override
	public URI root() {
		return root;
	}

	@Override
	public VolatileReadData createReadData(final URI normalPath) throws N5IOException {
		return VolatileReadData.from(new HttpLazyRead(root.resolve(normalPath)));
	}

	@Override
	public boolean isDirectory(final URI normalPath) {
		try {
			final URI uri = root.resolve(RootedURI.N5GroupPath.of(normalPath.getPath()).uri()); // TODO (N5Path): if we had isDirectory(N5GroupPath), we wouldn't have to do this
			requireValidHttpResponse(uri, HEAD, false, (code,  msg,http) -> {
				final N5Exception cause = validExistsResponse(code, "Error checking directory: " + normalPath, msg, true);
				if (code >= 300 && code < 400) {
					final String redirectLocation = http.getHeaderField("Location");
					if (!(redirectLocation.endsWith("/") || redirectLocation.endsWith("index.html")))
						return new N5NoSuchKeyException("Found File at " + normalPath + " but was not directory");
					return null;
				}
				return cause;
			});
			return true;
		} catch (N5Exception e) {
			return false;
		}
	}

	@Override
	public boolean isFile(final URI normalPath) {

		/* Files must not end in `/` And Don't accept a redirect to a location ending in `/` */
		try {
			final URI uri = root.resolve(RootedURI.N5FilePath.of(normalPath.getPath()).uri()); // TODO (N5Path): if we had isFile(N5FilePath), we wouldn't have to do this
			requireValidHttpResponse(uri, HEAD, false, (code, msg, http) -> {
				final N5Exception cause = validExistsResponse(code, "Error accessing file: " + normalPath, msg, true);
				if (code >= 300 && code < 400) {
					final String redirectLocation = http.getHeaderField("Location");
					if (redirectLocation.endsWith("/") || redirectLocation.endsWith("index.html"))
						return new N5NoSuchKeyException("Found key at " + normalPath + " but was directory");
				}
				return cause;
			});
			return true;
		} catch (N5Exception e) {
			return false;
		}
	}

	@Override
	public boolean exists(final URI normalPath) {

		try {
			requireValidHttpResponse(normalPath, HEAD, "Error checking existence: " + normalPath, true);
			return true;
		} catch (N5NoSuchKeyException e) {
			return false;
		}
	}

	@Override
	public long size(final URI normalPath) throws N5IOException {

		final HttpURLConnection head = requireValidHttpResponse(normalPath, HEAD, "Error checking existence: " + normalPath, true);
		return head.getContentLengthLong();
	}

	/**
	 * List all 'directory'-like children of a path.
	 * <p>
	 * Will throw an N5IOException both if a connection to the server can not be established, or the server does not allow listing.
	 *
	 * @param normalPath
	 *            is expected to be in normalized form, no further
	 *            efforts are made to normalize it.
	 * @return the directories
	 * @throws N5IOException
	 *             if an error occurs during listing
	 */
	@Override
	public String[] listDirectories(final URI normalPath) throws N5IOException {

		return queryListEntries(root.resolve(normalPath), listDirectoryResponseParser, true);
	}

	@Override
	public void write(final URI normalPath, final ReadData data) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}

	@Override
	public void createDirectories(final URI normalPath) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}

	@Override
	public void delete(final URI normalPath) throws N5IOException {
		throw new N5IOException("HttpKeyValueAccess is read-only");
	}



	// ------------------------------------------------------------------------
	//
	// --- copy & paste from HttpKeyValueAccess ---
	//

	private int readTimeoutMilliseconds = 5000;
	private int connectionTimeoutMilliseconds = 5000;

	private ListResponseParser listDirectoryResponseParser = ListResponseParser.defaultDirectoryListParser();

	public void setReadTimeout(int readTimeoutMilliseconds) {

		this.readTimeoutMilliseconds = readTimeoutMilliseconds;
	}

	public void setConnectionTimeout(int connectionTimeoutMilliseconds) {

		this.connectionTimeoutMilliseconds = connectionTimeoutMilliseconds;
	}

	public void setListDirectoryParser(final ListResponseParser parser) {

		listDirectoryResponseParser = parser;
	}

	private String[] queryListEntries(final URI uri, final ListResponseParser parser, final boolean allowRedirect) throws N5IOException{

		final HttpURLConnection http = requireValidHttpResponse(uri, GET, "Error listing directory at " + uri, allowRedirect);
		try {
			final String listResponse = responseToString(http.getInputStream());
			return parser.parseListResponse(listResponse);
		} catch (IOException e) {
			throw new N5IOException("Error listing directory at " + uri, e);
		}
	}

	private String responseToString(InputStream inputStream) throws IOException {

		return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
	}

	private HttpURLConnection httpRequest(URI uri, String method) throws IOException {

		final HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
		connection.setReadTimeout(readTimeoutMilliseconds);
		connection.setConnectTimeout(connectionTimeoutMilliseconds);
		connection.setRequestMethod(method);
		return connection;
	}

	private static N5Exception validExistsResponse(int code, String responseMsg, String message, boolean allowRedirect) {
		if (code >= 200 && code < (allowRedirect ? 400 : 300))
			return null;

		final String cause = message + "( " + responseMsg + ")(" + code + ")";
		if (code == 404 || code == 410)
			return new N5NoSuchKeyException(cause);
		else
			return new N5IOException(cause);
	}

	private HttpURLConnection requireValidHttpResponse(URI uri, String method, String message, boolean allowRedirect) throws N5Exception {
		return requireValidHttpResponse(uri, method, (code, msg, http) -> validExistsResponse(code, msg, message, allowRedirect));
	}

	private HttpURLConnection requireValidHttpResponse(URI uri, String method, TriFunction<Integer, String, HttpURLConnection, N5Exception> filterCode) throws N5Exception {
		return requireValidHttpResponse(uri, method, true, filterCode);
	}

	private HttpURLConnection requireValidHttpResponse(URI uri, String method, boolean followRedirects, TriFunction<Integer, String, HttpURLConnection, N5Exception> filterCode) throws N5Exception {

		final int code;
		final HttpURLConnection http;
		final String responseMsg;
		try {
			http = httpRequest(uri, method);
			http.setInstanceFollowRedirects(followRedirects);
			code = http.getResponseCode();
			responseMsg = http.getResponseMessage();
		} catch (IOException e) {
			throw new N5IOException("Could not validate HTTP Response", e);
		}

		// filterCode: (Integer, String, HttpURLConnection) -> N5Exception
		//             (http.getResponseCode(), http.getResponseMessage(), http) -> ?

		final N5Exception cause = filterCode.apply(code, responseMsg, http);
		if (cause != null) throw cause;
		return http;
	}

	private static final String HEAD = "HEAD";
	private static final String GET = "GET";

	private static final String RANGE = "Range";
	private static final String ACCEPT_RANGE = "Accept-Range";
	private static final String BYTES = "bytes";

	// TODO: Simplify! This doesn't have to implement LockedChannel or even Closeable. We only use the newInputStream() method.
	private class HttpObjectChannel implements LockedChannel {

		protected final URI uri;
		private final long startByte;
		private final long size;
		private final ArrayList<Closeable> resources = new ArrayList<>();

		protected HttpObjectChannel(final URI uri, long startByte, long size) {

			this.uri = uri;
			this.startByte = startByte;
			this.size = size;
		}

		private boolean isPartialRead() {
			return startByte > 0 || (size >= 0 && size != Long.MAX_VALUE);
		}

		@Override
		public InputStream newInputStream() throws N5IOException {

			try {
				HttpURLConnection conn = (HttpURLConnection)uri.toURL().openConnection();
				if (isPartialRead()) {
					conn.setRequestProperty(RANGE, rangeString());
					final String acceptRanges = conn.getHeaderField(ACCEPT_RANGE);
					if (acceptRanges == null || !acceptRanges.equals(BYTES)) {
						conn.disconnect();
						conn = (HttpURLConnection)uri.toURL().openConnection();
						return ReadData.from(conn.getInputStream()).materialize().slice(startByte, size).inputStream();
					}
				}
				return conn.getInputStream();
			} catch (FileNotFoundException e) {
				/*default HttpURLConnection throws FileNotFoundException on 404 or 410 */
				throw new N5NoSuchKeyException("Could not open stream for " + uri, e);
			} catch (IOException e) {
				throw new N5IOException("Could not open stream for " + uri, e);
			}
		}

		private String rangeString() {

			final String lastByte = (size > 0) ? Long.toString(startByte + size - 1) : "";
			return String.format("%s=%d-%s", BYTES, startByte, lastByte);
		}

		@Override
		public Reader newReader() {

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

	private class HttpLazyRead implements LazyRead {

		private final URI uri;

		HttpLazyRead(final URI uri) {
			this.uri = uri;
		}

		@Override
		public long size() {

			final HttpURLConnection head = requireValidHttpResponse(uri, "HEAD", "Error checking existence: " + uri, true);
			return head.getContentLengthLong();
		}

		@Override
		public ReadData materialize(long offset, long length) {
			try (final HttpObjectChannel ch = new HttpObjectChannel(uri, offset, length)) {
				return ReadData.from(ch.newInputStream()).materialize();
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		@Override
		public void close() {
		}
	}

}
