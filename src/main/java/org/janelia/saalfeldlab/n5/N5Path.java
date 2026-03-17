package org.janelia.saalfeldlab.n5;

import java.net.URI;

/**
 * A relative path (typically, the path of a dataset or group relative to
 * the container root).
 */
public interface N5Path {

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
	 * Returns the parent path, or {@code null} if this path is empty (i.e.,
	 * the parent would be {@code ".."}.
	 *
	 * @return the parent path, or {@code null} if this path is empty
	 */
	default N5GroupPath parent() {
		final URI parent = uri().resolve("..");
		return "..".equals(parent.getPath()) ? null : new N5PathImpl.GroupPath(parent);
	}

	N5GroupPath asGroup();

	N5FilePath asFile();

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

	interface N5GroupPath extends N5Path {

		@Override
		default boolean isGroup() {
			return true;
		}

		static N5GroupPath of(final String path) {
			return N5PathImpl.GroupPath.of(path);
		}
	}

	interface N5FilePath extends N5Path {

		@Override
		default boolean isGroup() {
			return false;
		}

		static N5FilePath of(final String path) {
			return N5PathImpl.FilePath.of(path);
		}

	}
}
