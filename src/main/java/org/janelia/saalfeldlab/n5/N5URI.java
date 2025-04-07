package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link URI} for N5 containers, groups, datasets, and attributes.
 * <p>
 * Container paths are stored in the URI path. Group / dataset paths are stored in the URI query,
 * and attribute paths are stored in the URI fragment.
 */
public class N5URI {

	private static final Charset UTF8 = Charset.forName("UTF-8");
	public static final Pattern ARRAY_INDEX = Pattern.compile("\\[([0-9]+)]");
	final URI uri;
	private final String scheme;
	private final String container;
	private final String group;
	private final String attribute;

	public N5URI(final String uri) throws URISyntaxException {

		this(encodeAsUri(uri));
	}

	public N5URI(final URI uri) {

		this.uri = uri;
		scheme = uri.getScheme() == null ? null : uri.getScheme();
		final String schemeSpecificPartWithoutQuery = getSchemeSpecificPartWithoutQuery();
		if (uri.getScheme() == null) {
			container = schemeSpecificPartWithoutQuery.replaceFirst("//", "");
		} else {
			container = uri.getScheme() + ":" + schemeSpecificPartWithoutQuery;
		}
		group = uri.getQuery();
		attribute = decodeFragment(uri.getRawFragment());
	}

	/**
	 * @return the container path
	 */
	public String getContainerPath() {

		return container;
	}

	public URI getURI() {
		return uri;
	}

	/**
	 * @return the group path, or root ("/") if none was provided
	 */
	public String getGroupPath() {

		return group != null ? group : "/";
	}

	/**
	 * @return the normalized group path
	 */
	public String normalizeGroupPath() {

		return normalizeGroupPath(getGroupPath());
	}

	/**
	 * @return the attribute path, or root ("/") if none was provided
	 */
	public String getAttributePath() {

		return attribute != null ? attribute : "/";
	}

	/**
	 * @return the normalized attribute path
	 */
	public String normalizeAttributePath() {

		return normalizeAttributePath(getAttributePath());
	}

	/**
	 * Parse this {@link N5URI} as a {@link LinkedAttributePathToken}.
	 *
	 * @see N5URI#getAttributePathTokens(String)
	 * @return the linked attribute path token
	 */
	public LinkedAttributePathToken<?> getAttributePathTokens() {

		return getAttributePathTokens(normalizeAttributePath());
	}

	/**
	 * Parses the {@link String normalizedAttributePath} to a list of
	 * {@link LinkedAttributePathToken}.
	 * This is useful for traversing or constructing a json representation of
	 * the provided {@link String normalizedAttributePath}.
	 * Note that {@link String normalizedAttributePath} should be normalized
	 * prior to generating this list
	 *
	 * @param normalizedAttributePath
	 *            to parse into {@link LinkedAttributePathToken}s
	 * @return the head of the {@link LinkedAttributePathToken}s
	 */
	public static LinkedAttributePathToken<?> getAttributePathTokens(final String normalizedAttributePath) {

		final String[] attributePathParts = normalizedAttributePath.replaceAll("^/", "").split("(?<!\\\\)/");

		if (attributePathParts.length == 0 || Arrays.stream(attributePathParts).allMatch(String::isEmpty))
			return null;

		final AtomicReference<LinkedAttributePathToken<?>> firstTokenRef = new AtomicReference<>();
		final AtomicReference<LinkedAttributePathToken<?>> currentTokenRef = new AtomicReference<>();
		final Consumer<LinkedAttributePathToken<?>> updateCurrentToken = (newToken) -> {
			if (firstTokenRef.get() == null) {
				firstTokenRef.set(newToken);
				currentTokenRef.set(firstTokenRef.get());
			} else {
				final LinkedAttributePathToken<?> currentToken = currentTokenRef.get();
				currentToken.childToken = newToken;
				currentTokenRef.set(newToken);
			}
		};

		for (final String pathPart : attributePathParts) {
			final Matcher matcher = ARRAY_INDEX.matcher(pathPart);
			final LinkedAttributePathToken<?> newToken;
			if (matcher.matches()) {
				final int index = Integer.parseInt(matcher.group().replace("[", "").replace("]", ""));
				newToken = new LinkedAttributePathToken.ArrayAttributeToken(index);
			} else {
				final String pathPartUnEscaped = pathPart.replaceAll("\\\\/", "/").replaceAll("\\\\\\[", "[");
				newToken = new LinkedAttributePathToken.ObjectAttributeToken(pathPartUnEscaped);
			}
			updateCurrentToken.accept(newToken);
		}

		return firstTokenRef.get();
	}

	private String getSchemePart() {

		return scheme == null ? "" : scheme + "://";
	}

	private String getContainerPart() {

		return container;
	}

	private String getGroupPart() {

		return group == null ? "" : "?" + group;
	}

	private String getAttributePart() {

		return attribute == null ? "" : "#" + attribute;
	}

	@Override
	public String toString() {

		return getContainerPart() + getGroupPart() + getAttributePart();
	}

	private String getSchemeSpecificPartWithoutQuery() {

		/* Why not substring "?"? */
		return uri.getSchemeSpecificPart().replace("?" + uri.getQuery(), "");
	}

	/**
	 * N5URI is always considered absolute if a scheme is provided.
	 * If no scheme is provided, the N5URI is absolute if it starts with either
	 * "/" or "[A-Z]:"
	 *
	 * @return if the path for this N5URI is absolute
	 */
	public boolean isAbsolute() {

		if (scheme != null)
			return true;
		final String path = uri.getPath();
		if (!path.isEmpty()) {
			final char char0 = path.charAt(0);
			return char0 == '/' || (path.length() >= 2 && path.charAt(1) == ':' && char0 >= 'A' && char0 <= 'Z');
		}
		return false;
	}

	/**
	 * Generate a new N5URI which is the result of resolving {@link N5URI
	 * relativeN5Url} to this {@link N5URI}.
	 * If relativeN5Url is not relative to this N5URI, then the resulting N5URI
	 * is equivalent to relativeN5Url.
	 *
	 * @param relativeN5Url
	 *            N5URI to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 *             if the uri is malformed
	 */
	public N5URI resolve(final N5URI relativeN5Url) throws URISyntaxException {

		final URI thisUri = uri;
		final URI relativeUri = relativeN5Url.uri;

		final StringBuilder newUri = new StringBuilder();

		if (relativeUri.getScheme() != null) {
			return relativeN5Url;
		}
		final String thisScheme = thisUri.getScheme();
		if (thisScheme != null) {
			newUri.append(thisScheme).append(":");
		}

		if (relativeUri.getAuthority() != null) {
			newUri
					.append(relativeUri.getAuthority())
					.append(relativeUri.getPath())
					.append(relativeN5Url.getGroupPart())
					.append(relativeN5Url.getAttributePart());
			return new N5URI(newUri.toString());
		}
		final String thisAuthority = thisUri.getAuthority();
		if (thisAuthority != null) {
			newUri.append("//").append(thisAuthority);
		}

		final String path = relativeUri.getPath();
		if (!path.isEmpty()) {
			if (!relativeN5Url.isAbsolute()) {
				newUri.append(thisUri.getPath()).append('/');
			}
			newUri
					.append(path)
					.append(relativeN5Url.getGroupPart())
					.append(relativeN5Url.getAttributePart());
			return new N5URI(newUri.toString());
		}
		newUri.append(thisUri.getPath());

		final String query = relativeUri.getQuery();
		if (query != null) {
			if (query.charAt(0) != '/' && thisUri.getQuery() != null) {
				newUri.append(this.getGroupPart()).append('/');
				newUri.append(relativeUri.getQuery());
			} else {
				newUri.append(relativeN5Url.getGroupPart());
			}
			newUri.append(relativeN5Url.getAttributePart());
			return new N5URI(newUri.toString());
		}
		newUri.append(this.getGroupPart());

		final String fragment = relativeUri.getFragment();
		if (fragment != null) {
			if (fragment.charAt(0) != '/' && thisUri.getFragment() != null) {
				newUri.append(this.getAttributePart()).append('/');
			} else {
				newUri.append(relativeN5Url.getAttributePart());
			}

			return new N5URI(newUri.toString());
		}
		newUri.append(this.getAttributePart());

		return new N5URI(newUri.toString());
	}

	/**
	 * Generate a new N5URI which is the result of resolving {@link URI
	 * relativeUri} to this {@link N5URI}.
	 * If relativeUri is not relative to this N5URI, then the resulting N5URI is
	 * equivalent to relativeUri.
	 *
	 * @param relativeUri
	 *            URI to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 *             if the uri is malformed
	 */
	public N5URI resolve(final URI relativeUri) throws URISyntaxException {

		return resolve(new N5URI(relativeUri));
	}

	/**
	 * Generate a new N5URI which is the result of resolving {@link String
	 * relativeString} to this {@link N5URI}
	 * If relativeString is not relative to this N5URI, then the resulting N5URI
	 * is equivalent to relativeString.
	 *
	 * @param relativeString
	 *            String to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 *             if the uri is malformed
	 */
	public N5URI resolve(final String relativeString) throws URISyntaxException {

		return resolve(new N5URI(relativeString));
	}

	/**
	 * Normalize a POSIX path, resulting in removal of redundant "/", "./", and
	 * resolution of relative "../".
	 * <p>
	 * NOTE: currently a private helper method only used by {@link N5URI#normalizeGroupPath(String)}.
	 * 	It's safe to do in that case since relative group paths should always be POSIX compliant.
	 * 	A new helper method to understand other path types (e.g. Windows) may be necessary eventually.
	 *
	 * @param path
	 *            to normalize
	 * @return the normalized path
	 */
	private static String normalizePath(String path) {

		path = path == null ? "" : path;
		final char[] pathChars = path.toCharArray();

		final List<String> tokens = new ArrayList<>();
		final StringBuilder curToken = new StringBuilder();
		boolean escape = false;
		for (final char character : pathChars) {
			/* Skip if we last saw escape */
			if (escape) {
				escape = false;
				curToken.append(character);
				continue;
			}
			/* Check if we are escape character */
			if (character == '\\') {
				escape = true;
			} else if (character == '/') {
				if (tokens.isEmpty() && curToken.length() == 0) {
					/* If we are root, and the first token, then add the '/' */
					curToken.append(character);
				}

				/*
				 * The current token is complete, add it to the list, if it
				 * isn't empty
				 */
				final String newToken = curToken.toString();
				if (!newToken.isEmpty()) {
					/*
					 * If our token is '..' then remove the last token instead
					 * of adding a new one
					 */
					if (newToken.equals("..")) {
						tokens.remove(tokens.size() - 1);
					} else {
						tokens.add(newToken);
					}
				}
				/* reset for the next token */
				curToken.setLength(0);
			} else {
				curToken.append(character);
			}
		}
		final String lastToken = curToken.toString();
		if (!lastToken.isEmpty()) {
			if (lastToken.equals("..")) {
				tokens.remove(tokens.size() - 1);
			} else {
				tokens.add(lastToken);
			}
		}
		if (tokens.isEmpty())
			return "";
		String root = "";
		if (tokens.get(0).equals("/")) {
			tokens.remove(0);
			root = "/";
		}
		return root + tokens
				.stream()
				.filter(it -> !it.equals("."))
				.filter(it -> !it.isEmpty())
				.reduce((l, r) -> l + "/" + r)
				.orElse("");
	}

	/**
	 * Normalize a group path relative to a container's root, resulting in
	 * removal of redundant "/", "./", resolution of relative "../",
	 * and removal of leading slashes.
	 *
	 * @param path
	 *            to normalize
	 * @return the normalized path
	 */
	public static String normalizeGroupPath(final String path) {

		/*
		 * Alternatively, could do something like the below in every
		 * KeyValueReader implementation
		 *
		 * return keyValueAccess.relativize( N5URI.normalizeGroupPath(path),
		 * basePath);
		 *
		 * has to be in the implementations, since KeyValueAccess doesn't have a
		 * basePath.
		 */
		return normalizePath(path.startsWith("/") || path.startsWith("\\") ? path.substring(1) : path);
	}

	/**
	 * Normalize the {@link String attributePath}.
	 * <p>
	 * Attribute paths have a few of special characters:
	 * <ul>
	 * <li>"." which represents the current element</li>
	 * <li>".." which represent the previous elemnt</li>
	 * <li>"/" which is used to separate elements in the json tree</li>
	 * <li>[N] where N is an integer, refer to an index in the previous element
	 * in the tree; the previous element must be an array.
	 * <p>
	 * Note: [N] also separates the previous and following elements, regardless
	 * of whether it is preceded by "/" or not.
	 * </li>
	 * <li>"\" which is an escape character, which indicates the subquent '/' or
	 * '[N]' should not be interpreted as a path delimeter,
	 * but as part of the current path name.</li>
	 *
	 * </ul>
	 * <p>
	 * When normalizing:
	 * <ul>
	 * <li>"/" are added before and after any indexing brackets [N]</li>
	 * <li>any redundant "/" are removed</li>
	 * <li>any relative ".." and "." are resolved</li>
	 * </ul>
	 * <p>
	 * Examples of valid attribute paths, and their normalizations
	 * <ul>
	 * <li>/a/b/c becomes /a/b/c</li>
	 * <li>/a/b/c/ becomes /a/b/c</li>
	 * <li>///a///b///c becomes /a/b/c</li>
	 * <li>/a/././b/c becomes /a/b/c</li>
	 * <li>/a/b[1]c becomes /a/b/[1]/c</li>
	 * <li>/a/b/[1]c becomes /a/b/[1]/c</li>
	 * <li>/a/b[1]/c becomes /a/b/[1]/c</li>
	 * <li>/a/b[1]/c/.. becomes /a/b/[1]</li>
	 * </ul>
	 *
	 * @param attributePath
	 *            to normalize
	 * @return the normalized attribute path
	 */
	public static String normalizeAttributePath(final String attributePath) {

		/*
		 * Short circuit if there are no non-escaped `/` or array indices (e.g.
		 * [N] where N is a non-negative integer)
		 */
		if (!attributePath.matches(".*((?<!\\\\)(/|\\[[0-9]+])).*")) {
			return attributePath;
		}

		/* Add separator after arrays at the beginning `[10]b` -> `[10]/b` */
		final String attrPathPlusFirstIndexSeparator = attributePath.replaceAll("^(?<array>\\[[0-9]+])", "${array}/");
		/*
		 * Add separator before and after arrays not at the beginning `a[10]b`
		 * -> `a/[10]/b`
		 */
		final String attrPathPlusIndexSeparators = attrPathPlusFirstIndexSeparator
				.replaceAll("((?<!(^|\\\\))(?<array>\\[[0-9]+]))", "/${array}/");

		/*
		 * The following has 4 possible matches, in each case it removes the
		 * match:
		 * The first 3 remove redundant separators of the form:
		 * 1.`a///b` -> `a/b` : (?<=/)/+
		 * 2.`a/./b` -> `a/b` : (?<=(/|^))(\./)+
		 * 3.`a/b/` -> `a/b` : ((/|(?<=/))\.)$
		 * The next avoids removing `/` when it is NOT redundant (e.g. only character, or escaped):
		 * 4. `/` -> `/ , `/a/b/\\/` -> `/a/b/\\/` : (?<!(^|\\))/$
		 * The last resolves relative paths:
		 * 5. ((?<=^/)|^|(?<=(/|^))[^/]+(?<!(/|(/|^)\.\.))/)\.\./?
		 * 		- `a/../b` -> `b`
		 * 		- `/a/../b` -> `/b`
		 * 		- `../a/../b` -> `b`
		 * 		- `/../a/../b` -> `/b`
		 * 		- `/../a/../../b` -> `/b`
		 *
		 * This is run iteratively, since earlier removals may cause later
		 * removals to be valid,
		 * as well as the need to match once per relative `../` pattern.
		 */
		final Pattern relativePathPattern = Pattern.compile(
						"((?<=/)/+|(?<=(/|^))(\\./)+|((/|(?<=/))\\.)$|(?<!(^|\\\\))/$|((?<=^/)|^|(?<=(/|^))[^/]+(?<!(/|(/|^)\\.\\.))/)\\.\\./?)"
				);
		int prevStringLenth = 0;
		String resolvedAttributePath = attrPathPlusIndexSeparators;
		while (prevStringLenth != resolvedAttributePath.length()) {
			prevStringLenth = resolvedAttributePath.length();
			resolvedAttributePath = relativePathPattern.matcher(resolvedAttributePath).replaceAll("");
		}
		return resolvedAttributePath;
	}

	/**
	 * If uri is a valid URI, just return it as a URI. Else, encode if possible.
	 *
	 * @param uri as String to get as URI
	 * @return URI from input. Encoded if necessary
	 */
	public static URI getAsUri(final String uri) {

		try {
			return URI.create(uri);
		} catch (Exception ignore) {
			try {
				return N5URI.encodeAsUri(uri);
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Could not encode as URI: " + uri, e);
			}
		}
	}

	public static URI encodeAsUriPath(final String path) {
		try {
			return new URI(null, null, path, null);
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not encode as URI path component:  " + path, e);
		}
	}

	/**
	 * Encode the inpurt {@link String uri} so that illegal characters are
	 * properly escaped prior to generating the resulting {@link URI}.
	 *
	 * @param uri
	 *            to encode
	 * @return the {@link URI} created from encoding the {@link String uri}
	 */
	public static URI encodeAsUri(final String uri) throws URISyntaxException {

		if (uri.trim().length() == 0) {
			//TODO Caleb: ???
			return new URI(uri);
		}
		/*
		 * find last # symbol to split fragment on. If we don't remove it first,
		 * then it will encode it, and not parse it separately
		 * after we remove the temporary _N5 scheme
		 */
		final int fragmentIdx = uri.lastIndexOf('#');
		final String uriWithoutFragment;
		final String fragment;
		if (fragmentIdx >= 0) {
			uriWithoutFragment = uri.substring(0, fragmentIdx);
			fragment = uri.substring(fragmentIdx + 1);
		} else {
			uriWithoutFragment = uri;
			fragment = null;
		}
		/* Edge case to handle when uriWithoutFragment is empty */
		final URI _n5Uri;
		if (uriWithoutFragment.length() == 0 && fragment != null && fragment.length() > 0) {
			_n5Uri = new URI("N5Internal", "//STAND_IN", fragment);
		} else {
			_n5Uri = new URI("N5Internal", uriWithoutFragment, fragment);
		}

		final URI n5Uri;
		if (fragment == null) {
			n5Uri = new URI(_n5Uri.getRawSchemeSpecificPart());
		} else {
			if (Objects.equals(_n5Uri.getPath(), "") && Objects.equals(_n5Uri.getAuthority(), "STAND_IN")) {
				n5Uri = new URI("#" + _n5Uri.getRawFragment());
			} else {
				n5Uri = new URI(_n5Uri.getRawSchemeSpecificPart() + "#" + _n5Uri.getRawFragment());
			}
		}
		return n5Uri;
	}

	/**
	 * Generate an {@link N5URI} from a container, group, and attribute
	 *
	 * @param container
	 *            of the N5Url
	 * @param group
	 *            of the N5Url
	 * @param attribute
	 *            of the N5Url
	 * @return the {@link N5URI}
	 * @throws URISyntaxException
	 *             if the uri is malformed
	 */
	public static N5URI from(
			final String container,
			final String group,
			final String attribute) throws URISyntaxException {

		final String containerPart = container != null ? container : "";
		final String groupPart = group != null ? "?" + group : "";
		final String attributePart = attribute != null ? "#" + attribute : "";
		return new N5URI(containerPart + groupPart + attributePart);
	}

	/**
	 * Intentionally copied from {@link URI} for internal use
	 *
	 * @see URI#decode(char)
	 */
	@SuppressWarnings("JavadocReference")
	private static int decode(final char c) {

		if ((c >= '0') && (c <= '9'))
			return c - '0';
		if ((c >= 'a') && (c <= 'f'))
			return c - 'a' + 10;
		if ((c >= 'A') && (c <= 'F'))
			return c - 'A' + 10;
		assert false;
		return -1;
	}

	/**
	 * Intentionally copied from {@link URI} for internal use
	 *
	 * @see URI#decode(char, char)
	 */
	@SuppressWarnings("JavadocReference")
	private static byte decode(final char c1, final char c2) {

		return (byte)(((decode(c1) & 0xf) << 4)
				| ((decode(c2) & 0xf) << 0));
	}

	/**
	 * Modified from {@link URI#decode(String)} to ignore the listed exception,
	 * where it doesn't decode escape values inside square braces.
	 * <p>
	 * As an example of the original implementation, a backslash inside a square
	 * brace would be encoded to "[%5C]", and
	 * when calling {@code decode("[%5C]")} it would not decode to "[\]" since
	 * the encode escape sequence is inside square braces.
	 * <p>
	 * We keep all the decoding logic in this modified version, EXCEPT, that we
	 * don't check for and ignore encoded sequences inside square braces.
	 * <p>
	 * Thus, {@code decode("[%5C]")} -> "[\]".
	 *
	 * @see URI#decode(char, char)
	 */
	@SuppressWarnings("JavadocReference")
	private static String decodeFragment(final String rawFragment) {

		if (rawFragment == null)
			return rawFragment;
		final int n = rawFragment.length();
		if (n == 0)
			return rawFragment;
		if (rawFragment.indexOf('%') < 0)
			return rawFragment;

		final StringBuffer sb = new StringBuffer(n);
		final ByteBuffer bb = ByteBuffer.allocate(n);
		final CharBuffer cb = CharBuffer.allocate(n);

		final CharsetDecoder dec = UTF8
				.newDecoder()
				.onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE);

		// This is not horribly efficient, but it will do for now
		char c = rawFragment.charAt(0);

		for (int i = 0; i < n;) {
			assert c == rawFragment.charAt(i); // Loop invariant
			if (c != '%') {
				sb.append(c);
				if (++i >= n)
					break;
				c = rawFragment.charAt(i);
				continue;
			}
			bb.clear();
			for (;;) {
				assert (n - i >= 2);
				bb.put(decode(rawFragment.charAt(++i), rawFragment.charAt(++i)));
				if (++i >= n)
					break;
				c = rawFragment.charAt(i);
				if (c != '%')
					break;
			}
			bb.flip();
			cb.clear();
			dec.reset();
			CoderResult cr = dec.decode(bb, cb, true);
			assert cr.isUnderflow();
			cr = dec.flush(cb);
			assert cr.isUnderflow();
			sb.append(cb.flip());
		}

		return sb.toString();
	}

}
