package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
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
		default String path() {
			throw new UnsupportedOperationException("TODO: decide what this should do...");
		}

		/**
		 * Return this {@code N5Path} as a relative path without leading or trailing slashes.
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
			return "..".equals(parent.getPath()) ? null : new GroupPath(parent);
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
	}

	private static class GroupPath implements N5GroupPath {

		private final URI uri;

		private transient String normalPath;

		public GroupPath(final URI uri) {
			this.uri = uri;
		}

		@Override
		public boolean isGroup() {
			return true;
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
			return "..".equals(parent.getPath()) ? null : new GroupPath(parent);
		}

		@Override
		public String toString() {
			return "{group \"" + uri + "\"}";
		}
	}

	//
	public interface N5FilePath extends N5Path {

		static N5FilePath of(final String path) {
			String p = createURI(path).normalize().getPath();
			if (p.startsWith("/"))
				p = p.substring(1);
			if (p.endsWith("/"))
				p = p.substring(0, p.length() - 1);
			return new FilePath(createURI(p));
		}
	}

	private static class FilePath implements N5FilePath {

		private final URI uri;

		public FilePath(final URI uri) {
			if (uri.getPath().isEmpty())
				throw new IllegalArgumentException("invalid empty file URI");
			this.uri = uri;
		}

		@Override
		public boolean isGroup() {
			return false;
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
		public String toString() {
			return "{file \"" + uri + "\"}";
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

//		group("");
//		group("./");
//		group("/");
//		group("/./");
//		group("././/");
//		group("/////");
//		group("/a/b/c/");
//		group("/a/b/c");
//		group("a/b/c/");
//		group("a/b/c");

		parent("");
		parent("./");
		parent("/");
		parent("/./");
		parent("././/");
		parent("/////");
		parent("/a/b/c/");
		parent("/a/b/c");
		parent("a/b/c/");
		parent("a/b/c");
		parent("a/");
		parent("a");
	}

	private static void group(String str) {
		System.out.println("\"" + str + "\" --> " + N5GroupPath.of(str));
	}

	private static void parent(String str) {
		final N5GroupPath group = N5GroupPath.of(str);
		System.out.println("\"" + str + "\" --> " + group + " --> " + group.parent());
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
