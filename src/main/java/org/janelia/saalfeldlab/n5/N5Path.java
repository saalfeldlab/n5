package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;

import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.filePathOf;
import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.groupPathOf;
import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.pathOf;

/**
 * A relative path (typically, the path of a file or group relative to the
 * container root).
 */
//TODO: Currently, {@code N5Path} is allowed to point outside the root (e.g.,
// {@code "../a/"}), though this is not used internally and should probably be
// explicitly forbidden.
public interface N5Path {

	static N5Path of(final String path) {
		return pathOf(path);
	}

	/**
	 * Return this path as a {@code N5GroupPath}. If this path is not a already
	 * {@link #isGroup() group}, appends a "/".
	 *
	 * @return this path as a group (appending "/" if necessary).
	 */
	N5GroupPath asGroup();

	/**
	 * Return this path as a {@code N5FilePath}. If this path is a already
	 * {@link #isGroup() group}, remove the trailing "/".
	 * <p>
	 * If this path is the root group ({@code ""}) then it cannot be treated as
	 * a file path.
	 *
	 * @return this path as a file (removing trailing "/" if necessary).
	 *
	 * @throws IllegalArgumentException
	 * 		if this path represents the root group {@code ""}
	 */
	N5FilePath asFile() throws IllegalArgumentException;

	/**
	 * Return this {@code N5Path} as a relative URI with a relative path.
	 * <p>
	 * If this {@code N5Path} {@link #isGroup() represents a group} the URI
	 * path will have a trailing slash (or be empty). Otherwise, if this
	 * {@code N5Path} represents a normal file, the URI will be non-empty and
	 * will not have a trailing slash.
	 * <p>
	 * The URI is normalized.
	 *
	 * @return this N5Path as a normalized relative URI
	 */
	URI uri();

	/**
	 * Return this {@code N5Path} as a relative path without leading slashes.
	 * If this {@code N5Path} is a file the path will have no trailing slash. If
	 * this {@code N5Path} is a {@link #isGroup() group} the path will have a
	 * trailing slash, unless it represents the root group ({@code ""}).
	 * <p>
	 * Note that in contrast to the URI representation, special characters are
	 * not escaped.
	 */
	default String path() {
		return uri().getPath();
	}

	/**
	 * Return this {@code N5Path} as a relative path without leading or trailing slashes.
	 * <p>
	 * Note that in contrast to the URI representation, special characters are not escaped.
	 */
	String normalPath();

	// basically: whether the URI ends with a slash...
	boolean isGroup();

	/**
	 * Returns the parent path, or {@code null} if this path is the root group
	 * (i.e., the parent would be {@code ".."}.
	 *
	 * @return the parent path, or {@code null} if this path is empty
	 */
	N5GroupPath parent();

	/**
	 * Split this path into components separated by {@code "/"}.
	 * <p>
	 * Note, that the components are derived from the string representation,
	 * so special characters in components are not escaped.
	 * <p>
	 * If this path is empty (that is {@code ""}, which is equivalent to the
	 * group "./") then the returned array will be {@code {""}}.
	 * <p>
	 * TODO: add examples.
	 *
	 * @return components of this path
	 */
	default String[] components() {
		return uri().getPath().split("/");
	}

	/**
	 * A relative path representing a directory (typically, the path of a group
	 * relative to the container root).
	 * <p>
	 * The {@link #uri()} has a trailing slash (or be empty).
	 */
	interface N5GroupPath extends N5Path {

		@Override
		default boolean isGroup() {
			return true;
		}

		default N5Path resolve(final String path) {
			return N5Path.of(uri().resolve(path).getPath());
		}

		static N5GroupPath of(final String path) {
			return groupPathOf(path);
		}
	}

	/**
	 * A relative path representing a file (typically, the path of a file
	 * relative to the container root).
	 * <p>
	 * The {@link #uri()} is non-empty and has a trailing slash.
	 */
	interface N5FilePath extends N5Path {

		@Override
		default boolean isGroup() {
			return false;
		}

		static N5FilePath of(final String path) {
			return filePathOf(path);
		}
	}

	// ------------------------------------------------------------------------
	// Implementation.
	//
	// TODO: Make private when moving to newer Java version
	class N5PathImpl {

		private N5PathImpl() {
			// do not instantiate
		}

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

		private static URI createURI(final String normalPath) throws N5Exception.N5IOException {

			try {
				return new URI(null, null, normalPath, null);
			} catch (URISyntaxException e) {
				// This should be unreachable: Scheme/authority/fragment are null
				// and the path component accepts virtually anything.
				throw new N5Exception.N5IOException(e);
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
}
