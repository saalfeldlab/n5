package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * TEMPORARY helper.
 * TODO: clean up later...
 * <p>
 * Collect methods for turning Strings into URI, handling expected URI types, etc.
 * This is cherry-picked from N5URI etc
 */
public class RootedURI {


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
		 * {@code N5Path} represents a dataset, the URI will be non-empty and
		 * will not have a trailing slash.
		 * <p>
		 * The URI is normalized.
		 *
		 * @return this N5Path as a normalized relative URI
		 */
		URI uri();

		/**
		 * @return TODO
		 *
		 * TODO: Note that in contrast to the URI representation, special characters are not escaped.
		 * TODO: Decide what this should return. That is, should path for groups end with a slash?
		 */
		String path();

		/**
		 * Return this {@code N5Path} as a relative path without leading or trailing slashes.
		 */
		String normalPath();

		// basically: whether the URI ends with a slash...
		boolean isGroup();

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

		// recursively enumerate parents of this URI
		default List<N5Path> prefixes() {
			final List<N5Path> prefixes = new ArrayList<N5Path>();
			return prefixes;
		}
	}

	public interface N5GroupPath extends N5Path {

		static N5GroupPath of(final String path) {
			String p = createURI(path).normalize().getPath();
			if (p.startsWith("/"))
				p = p.substring(1);
			if (!p.isEmpty() && !p.endsWith("/"))
				p = p + "/";
			return new GroupPath(createURI(p));
		}

		default boolean isGroup() {
			return true;
		}
	}

	private static class GroupPath implements N5GroupPath {

		private final URI uri;

		private transient String normalPath;

		public GroupPath(final URI uri) {
			this.uri = uri;
		}

		@Override
		public URI uri() {
			return uri;
		}

		@Override
		public String path() {
			throw new UnsupportedOperationException("TODO: decide what this should do...");
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
		public String toString() {
			return "{group \"" + uri + "\"}";
		}
	}

	//
	public interface DatasetURI extends N5Path {

		default boolean isGroup() {
			return false;
		}
	}

	// just playing around...
	public static void main(String[] args) {
//		split("/a/b/c/");
//		split("/a/b/c");
//		split("a/b/c/");
//		split("a/b/c");
//		split("");
//		split("/"); // this will not happen

		group("");
		group("./");
		group("/");
		group("/./");
		group("././/");
		group("/////");
		group("/a/b/c/");
		group("/a/b/c");
		group("a/b/c/");
		group("a/b/c");
	}

	private static void group(String str) {
		System.out.println("\"" + str + "\" --> " + N5GroupPath.of(str));
	}

	private static void uri(String str) {
		final URI uri = createURI(str);
		System.out.println("\"" + str + "\" --> \"" + uri + "\" --> \"" + uri.normalize() + "\"");
	}

	private static void split(String str) {
		final String[] split = str.split("/");
		System.out.println( "\"" + str + "\" --> " + Arrays.toString(split) + split.length);
	}

	// -- from N5URI -- //

	/**
	 * Normalize a group path relative to a container's root, resulting in
	 * removal of redundant "/", "./", resolution of relative "../",
	 * and removal of leading and trailing slashes.
	 *
	 * @param path
	 *            to normalize
	 * @return the normalized path
	 */
	public static String normalizeGroupPath(final String path) {

		String normalized = normalizePath(path);
		normalized = normalized.startsWith("/") ? normalized.substring(1) : normalized;
		return normalized.endsWith("/") ? normalized.substring(0, normalized.length() - 1) : normalized;
	}

	/**
	 * Normalize {@code path}, resulting in removal of redundant "/", "./", and
	 * resolution of relative "../".
	 *
	 * @param path
	 *            to normalize
	 * @return the normalized path
	 */
	private static String normalizePath(String path) {

		// TODO: getPath() returns the decoded path, undoing any percent-encoding the createURI applied
		//       Should we use toString() instead?
		//       Should we just return URI?
		return path.isEmpty() ? "" : createURI(path).normalize().getPath();
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

}
