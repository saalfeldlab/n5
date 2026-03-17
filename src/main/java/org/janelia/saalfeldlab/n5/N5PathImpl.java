package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

/**
 * TEMPORARY helper.
 * TODO: clean up later...
 * <p>
 * Collect methods for turning Strings into URI, handling expected URI types, etc.
 * This is cherry-picked from N5URI etc
 */
// TODO rename
class N5PathImpl {

	static N5Path pathOf(final String path) {

		final String p = normalize(path);
		if (p.isEmpty() || p.endsWith("/"))
			return new GroupPath(createURI(p));
		else if (p.equals("..") || p.endsWith("/.."))
			return new GroupPath(createURI(p + "/"));
		else
			return new FilePath(createURI(p));
	}

	static FilePath filePathOf(final String path) {

		String p = normalize(path);
		if (p.endsWith("/"))
			p = p.substring(0, p.length() - 1);

		final URI uri = createURI(p);
		if (uri.getPath().isEmpty())
			throw new IllegalArgumentException("invalid path \"" + path + "\" resolves to empty file path");

		return new FilePath(uri);
	}

	static GroupPath groupPathOf(final String path) {

		String p = normalize(path);
		if (!p.isEmpty() && !p.endsWith("/"))
			p = p + "/";

		return new GroupPath(createURI(p));
	}

	static class GroupPath implements N5GroupPath {

		private final URI uri;

		private transient String normalPath;

		private GroupPath(final URI uri) {
			this.uri = uri;
		}

		@Override
		public N5GroupPath asGroup() {
			return this;
		}

		@Override
		public N5FilePath asFile() {
			return new FilePath(createURI(normalPath()));
		}

		@Override
		public URI uri() {
			return uri;
		}

		@Override
		public String normalPath() {
			if (normalPath == null) {
				final String p = uri.getPath();
				normalPath = p.isEmpty() ? p : p.substring(0, p.length() - 1);
			}
			return normalPath;
		}

		@Override
		public N5GroupPath parent() {
			final URI parent = uri.resolve("..");
			final String path = parent.getPath();
			if ("..".equals(path))
				return null;
			if (!path.isEmpty() && !path.endsWith("/"))
				return new GroupPath(createURI(path + "/"));
			else
				return new GroupPath(parent);
		}

		@Override
		public String toString() {
			return "{group \"" + uri + "\"}";
		}
	}

	static class FilePath implements N5FilePath {

		private final URI uri;

		private FilePath(final URI uri) {
			if (uri.getPath().isEmpty())
				throw new IllegalArgumentException("invalid empty file path");
			this.uri = uri;
		}

		@Override
		public N5GroupPath asGroup() {
			return new GroupPath(createURI(normalPath() + "/"));
		}

		@Override
		public N5FilePath asFile() {
			return this;
		}

		@Override
		public URI uri() {
			return uri;
		}

		@Override
		public String normalPath() {
			return uri.getPath();
		}

		@Override
		public N5GroupPath parent() {
			final URI parent = uri.resolve(".");
			return "..".equals(parent.getPath()) ? null : new GroupPath(parent);
		}

		@Override
		public String toString() {
			return "{file \"" + uri + "\"}";
		}
	}

	private static URI createURI(final String normalPath) throws N5IOException {

		try {
			return new URI(null, null, normalPath, null);
		} catch (URISyntaxException e) {
			// This should be unreachable: Scheme/authority/fragment are null
			// and the path component accepts virtually anything.
			throw new N5IOException(e);
		}
	}

	/**
	 * Normalize a POSIX path, for use with {@link N5GroupPath#of},  {@link N5FilePath#of}, TODO.
	 * <p>
	 * Remove (any number of) leading slashes.
	 * Remove redundant "/", "./", and resolution of relative "../".
	 *
	 * @param path
	 *            to normalize
	 * @return the normalized path
	 */
	private static String normalize(String path) {

		// strip leading slashes
		int start = 0;
		while (start < path.length() && path.charAt(start) == '/') {
			start++;
		}

		// normalize
		return createURI(path.substring(start)).normalize().getPath();
	}

}
