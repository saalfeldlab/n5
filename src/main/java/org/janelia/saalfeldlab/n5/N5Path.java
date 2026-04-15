package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;

import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.filePathOf;
import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.directoryPathOf;
import static org.janelia.saalfeldlab.n5.N5Path.N5PathImpl.pathOf;

/**
 * A relative path (typically, the path of a file or directory relative to the
 * container root).
 */
//TODO: Currently, {@code N5Path} is allowed to point outside the root (e.g.,
// {@code "../a/"}), though this is not used internally and should probably be
// explicitly forbidden.
public interface N5Path {

	/**
	 * Create a {@code N5Path} from the given {@code path} string.
	 * <p>
	 * The given {@code path} will be normalized by removing (any number of)
	 * leading slashes, removing redundant "/", "./", and resolving relative
	 * "../".
	 * <p>
	 * If, after normalization, there is a trailing "/" (or the path is empty)
	 * then a {@code N5DirectoryPath} will be returned. Otherwise, a {@code
	 * N5FilePath} will be returned
	 *
	 * @param path
	 * 		path of a file or directory relative to the container root
	 */
	static N5Path of(final String path) {
		return pathOf(path);
	}

	/**
	 * Return this path as a {@code N5DirectoryPath}. If this path is not a
	 * already {@link #isDirectory() directory}, appends a "/".
	 *
	 * @return this path as a directory (appending "/" if necessary).
	 */
	N5DirectoryPath asDirectory();

	/**
	 * Return this path as a {@code N5FilePath}. If this path is a {@link
	 * #isDirectory() directory}, remove the trailing "/".
	 * <p>
	 * If this path is the root directory({@code ""}) then it cannot be treated
	 * as a file path.
	 *
	 * @return this path as a file (removing trailing "/" if necessary).
	 *
	 * @throws IllegalArgumentException
	 * 		if this path represents the root directory {@code ""}
	 */
	N5FilePath asFile() throws IllegalArgumentException;

	/**
	 * Return this {@code N5Path} as a relative URI with a relative path.
	 * <p>
	 * If this {@code N5Path} {@link #isDirectory() represents a directory} the
	 * URI path will have a trailing slash (or be empty). Otherwise, if this
	 * {@code N5Path} represents a normal file, the URI will be non-empty and
	 * will not have a trailing slash.
	 * <p>
	 * The URI is normalized.
	 *
	 * @return this N5Path as a normalized relative URI
	 */
	URI uri();

	/**
	 * Return this {@code N5Path} as a relative path without leading slashes. If
	 * this {@code N5Path} is a file the path will have no trailing slash. If
	 * this {@code N5Path} is a {@link #isDirectory() directory} the path will
	 * have a trailing slash, unless it represents the root directory ({@code ""}).
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
	boolean isDirectory();

	/**
	 * Returns the parent path, or {@code null} if this path is the root
	 * directory (i.e., the parent would be {@code ".."}.
	 *
	 * @return the parent path, or {@code null} if this path is empty
	 */
	N5DirectoryPath parent();

	/**
	 * Split this path into components separated by {@code "/"}.
	 * <p>
	 * Note, that the components are derived from the string representation,
	 * so special characters in components are not escaped.
	 * <p>
	 * If this path is empty (that is {@code ""}, which is equivalent to the
	 * directory "./") then the returned array will be {@code {""}}.
	 * <p>
	 * TODO: add examples.
	 *
	 * @return components of this path
	 */
	default String[] components() {
		return uri().getPath().split("/");
	}

	/**
	 * Split this path into components separated by {@code "/"}, and return the
	 * last component.
	 * <p>
	 * Note, that the components are derived from the string representation, so
	 * special characters in components are not escaped.
	 * <p>
	 * If this path is empty (that is {@code ""}, which is equivalent to the
	 * group "./") then {@code ""} is returned.
	 */
	default String filename() {
		final String[] c = components();
		return c[c.length - 1];
	}

	/**
	 * A relative path representing a directory (relative to the container root).
	 * <p>
	 * The {@link #uri()} has a trailing slash (or is empty).
	 */
	interface N5DirectoryPath extends N5Path {

		@Override
		default boolean isDirectory() {
			return true;
		}

		default N5Path resolve(final String path) {
			return path == null ? this : N5Path.of(uri().resolve(path).getPath());
		}

		/**
		 * Create a {@code N5DirectoryPath} from the given {@code path} string.
		 * <p>
		 * The given {@code path} will be normalized by removing (any number of)
		 * leading slashes, removing redundant "/", "./", and resolving relative
		 * "../".
		 *
		 * @param path
		 * 		directory path relative to the container root
		 */
		static N5DirectoryPath of(final String path) {
			return directoryPathOf(path);
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
		default boolean isDirectory() {
			return false;
		}

		/**
		 * Create a {@code N5FilePath} from the given {@code path} string.
		 * <p>
		 * The given {@code path} will be normalized by removing (any number of)
		 * leading slashes, removing redundant "/", "./", and resolving relative
		 * "../". If there is a trailing "/", it will be removed.
		 *
		 * @param path
		 * 		file path relative to the container root
		 */
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
				return new DirectoryPath(createURI(p));
			else if (p.equals("..") || p.endsWith("/.."))
				return new DirectoryPath(createURI(p + "/"));
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

		static DirectoryPath directoryPathOf(final String path) {

			String p = normalize(path);
			if (!p.isEmpty() && !p.endsWith("/"))
				p = p + "/";

			return new DirectoryPath(createURI(p));
		}

		private static class DirectoryPath implements N5DirectoryPath {

			private final URI uri;

			private transient String normalPath;

			private DirectoryPath(final URI uri) {
				this.uri = uri;
			}

			@Override
			public N5DirectoryPath asDirectory() {
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
			public N5DirectoryPath parent() {
				final URI parent = uri.resolve("..");
				final String path = parent.getPath();
				if ("..".equals(path))
					return null;
				if (!path.isEmpty() && !path.endsWith("/"))
					return new DirectoryPath(createURI(path + "/"));
				else
					return new DirectoryPath(parent);
			}

			@Override
			public String toString() {
				return "{group \"" + uri + "\"}";
			}

			@Override
			public final boolean equals(final Object o) {
				return (o instanceof N5DirectoryPath) && uri.equals(((N5DirectoryPath) o).uri());
			}

			@Override
			public int hashCode() {
				return uri.hashCode();
			}
		}

		private static class FilePath implements N5FilePath {

			private final URI uri;

			private FilePath(final URI uri) {
				if (uri.getPath().isEmpty())
					throw new IllegalArgumentException("invalid empty file path");
				this.uri = uri;
			}

			@Override
			public N5DirectoryPath asDirectory() {
				return new DirectoryPath(createURI(normalPath() + "/"));
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
			public N5DirectoryPath parent() {
				final URI parent = uri.resolve(".");
				return "..".equals(parent.getPath()) ? null : new DirectoryPath(parent);
			}

			@Override
			public String toString() {
				return "{file \"" + uri + "\"}";
			}

			@Override
			public final boolean equals(final Object o) {
				return (o instanceof N5FilePath) && uri.equals(((N5FilePath) o).uri());
			}

			@Override
			public int hashCode() {
				return uri.hashCode();
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
		 * Normalize a POSIX path, for use with {@link N5DirectoryPath#of},  {@link N5FilePath#of}, TODO.
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
