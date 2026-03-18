package org.janelia.saalfeldlab.n5;

import java.net.URI;
import org.janelia.saalfeldlab.n5.N5PathImpl.GroupPath;

import static org.janelia.saalfeldlab.n5.N5PathImpl.filePathOf;
import static org.janelia.saalfeldlab.n5.N5PathImpl.groupPathOf;
import static org.janelia.saalfeldlab.n5.N5PathImpl.pathOf;

/**
 * A relative path (typically, the path of a dataset or group relative to
 * the container root).
 *
 *
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

	interface N5FilePath extends N5Path {

		@Override
		default boolean isGroup() {
			return false;
		}

		static N5FilePath of(final String path) {
			return filePathOf(path);
		}

	}
}
