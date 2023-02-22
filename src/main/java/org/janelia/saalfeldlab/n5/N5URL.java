package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class N5URL {

	static final Pattern ARRAY_INDEX = Pattern.compile("\\[([0-9]+)]");
	final URI uri;
	private final String scheme;
	private final String container;
	private final String group;
	private final String attribute;

	public N5URL(String uri) throws URISyntaxException {

		this(encodeAsUri(uri));
	}

	public N5URL(URI uri) {

		this.uri = uri;
		scheme = uri.getScheme() == null ? null : uri.getScheme();
		final String schemeSpecificPartWithoutQuery = getSchemeSpecificPartWithoutQuery();
		if (uri.getScheme() == null) {
			container = schemeSpecificPartWithoutQuery.replaceFirst("//", "");
		} else {
			container = uri.getScheme() + ":" + schemeSpecificPartWithoutQuery;
		}
		group = uri.getQuery();
		attribute = uri.getFragment();
	}

	/**
	 * @return the container path
	 */
	public String getContainerPath() {

		return container;
	}

	/**
	 * @return the group path, or root ("/") if none was provided
	 */
	public String getGroupPath() {

		return group != null ? group : "/";
	}

	/**
	 * @return the normalized container path
	 */
	public String normalizeContainerPath() {

		return normalizePath(getContainerPath());
	}

	/**
	 * @return the normalized group path
	 */
	public String normalizeGroupPath() {

		return normalizePath(getGroupPath());
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
	 * Parse this {@link N5URL} as a {@link LinkedAttributePathToken}.
	 *
	 * @see N5URL#getAttributePathTokens(String)
	 */
	public LinkedAttributePathToken<?> getAttributePathTokens() {

		return getAttributePathTokens(normalizeAttributePath());
	}

	/**
	 * Parses the {@link String normalizedAttributePath} to a list of {@link LinkedAttributePathToken}.
	 * This is useful for traversing or constructing a json representation of the provided {@link String normalizedAttributePath}.
	 * Note that {@link String normalizedAttributePath} should be normalized prior to generating this list
	 *
	 * @param normalizedAttributePath to parse into {@link LinkedAttributePathToken}s
	 * @return the head of the {@link LinkedAttributePathToken}s
	 */
	public static LinkedAttributePathToken<?> getAttributePathTokens(String normalizedAttributePath) {

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

	@Override public String toString() {

		return getContainerPart() + getGroupPart() + getAttributePart();
	}

	private String getSchemeSpecificPartWithoutQuery() {

		/* Why not substring "?"?*/
		return uri.getSchemeSpecificPart().replace("?" + uri.getQuery(), "");
	}

	/**
	 * N5URL is always considered absolute if a scheme is provided.
	 * If no scheme is provided, the N5URL is absolute if it starts with either "/" or "[A-Z]:"
	 *
	 * @return if the path for this N5URL is absolute
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
	 * Generate a new N5URL which is the result of resolving {@link N5URL relativeN5Url} to this {@link N5URL}.
	 * If relativeN5Url is not relative to this N5URL, then the resulting N5URL is equivalent to relativeN5Url.
	 *
	 * @param relativeN5Url N5URL to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 */
	public N5URL resolve(N5URL relativeN5Url) throws URISyntaxException {

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
			return new N5URL(newUri.toString());
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
			return new N5URL(newUri.toString());
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
			return new N5URL(newUri.toString());
		}
		newUri.append(this.getGroupPart());

		final String fragment = relativeUri.getFragment();
		if (fragment != null) {
			if (fragment.charAt(0) != '/' && thisUri.getFragment() != null) {
				newUri.append(this.getAttributePart()).append('/');
			} else {
				newUri.append(relativeN5Url.getAttributePart());
			}

			return new N5URL(newUri.toString());
		}
		newUri.append(this.getAttributePart());

		return new N5URL(newUri.toString());
	}

	/**
	 * Generate a new N5URL which is the result of resolving {@link URI relativeUri} to this {@link N5URL}.
	 * If relativeUri is not relative to this N5URL, then the resulting N5URL is equivalent to relativeUri.
	 *
	 * @param relativeUri URI to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 */
	public N5URL resolve(URI relativeUri) throws URISyntaxException {

		return resolve(new N5URL(relativeUri));
	}

	/**
	 * Generate a new N5URL which is the result of resolving {@link String relativeString} to this {@link N5URL}
	 * If relativeString is not relative to this N5URL, then the resulting N5URL is equivalent to relativeString.
	 *
	 * @param relativeString String to resolve against ourselves
	 * @return the result of the resolution.
	 * @throws URISyntaxException
	 */
	public N5URL resolve(String relativeString) throws URISyntaxException {

		return resolve(new N5URL(relativeString));
	}

	/**
	 * Normalize a path, resulting in removal of redundant "/", "./", and resolution of relative "../".
	 *
	 * @param path to normalize
	 * @return the normalized path
	 */
	public static String normalizePath(String path) {

		path = path == null ? "" : path;
		final char[] pathChars = path.toCharArray();

		final List<String> tokens = new ArrayList<>();
		StringBuilder curToken = new StringBuilder();
		boolean escape = false;
		for (final char character : pathChars) {
			/* Skip if we last saw escape*/
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

				/* The current token is complete, add it to the list, if it isn't empty */
				final String newToken = curToken.toString();
				if (!newToken.isEmpty()) {
					/* If our token is '..' then remove the last token instead of adding a new one */
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
		return root + tokens.stream()
				.filter(it -> !it.equals("."))
				.filter(it -> !it.isEmpty())
				.reduce((l, r) -> l + "/" + r).orElse("");
	}

	/**
	 * Normalize the {@link String attributePath}.
	 * <p>
	 * Attribute paths have a few of special characters:
	 * <ul>
	 * 	<li>"." which represents the current element </li>
	 * 	<li>".." which represent the previous elemnt </li>
	 * 	<li>"/" which is used to separate elements in the json tree </li>
	 * 	<li>[N] where N is an integer, refer to an index in the previous element in the tree; the previous element must be an array. </li>
	 * 		Note: [N] also separates the previous and following elements, regardless of whether it is preceded by "/" or not.
	 * 	<li>"\" which is an escape character, which indicates the subquent '/' or '[N]' should not be interpreted as a path delimeter,
	 * 	but as part of the current path name. </li>
	 *
	 * </ul>
	 * 	<p>
	 * When normalizing:
	 * <ul>
	 * 	<li>"/" are added before and after any indexing brackets [N] </li>
	 * 	<li>any redundant "/" are removed </li>
	 * 	<li>any relative ".." and "." are resolved </li>
	 * </ul>
	 * <p>
	 * Examples of valid attribute paths, and their normalizations
	 * <ul>
	 * 	<li>/a/b/c -> /a/b/c</li>
	 * 	<li>/a/b/c/ -> /a/b/c</li>
	 * 	<li>///a///b///c -> /a/b/c</li>
	 * 	<li>/a/././b/c -> /a/b/c</li>
	 * 	<li>/a/b[1]c -> /a/b/[1]/c</li>
	 * 	<li>/a/b/[1]c -> /a/b/[1]/c</li>
	 * 	<li>/a/b[1]/c -> /a/b/[1]/c</li>
	 * 	<li>/a/b[1]/c/.. -> /a/b/[1]</li>
	 * </ul>
	 *
	 * @param attributePath to normalize
	 * @return the normalized attribute path
	 */
	public static String normalizeAttributePath(String attributePath) {

		final String attrPathPlusFirstIndexSeparator = attributePath.replaceAll("^(?<array>\\[[0-9]+])", "${array}/");
		final String attrPathPlusIndexSeparators = attrPathPlusFirstIndexSeparator.replaceAll("((?<prev>[^\\\\])(?<!^)(?<array>\\[[0-9]+]))",
				"${prev}/${array}/");
		final String attrPathRemoveMultipleSeparators = attrPathPlusIndexSeparators.replaceAll("(?<slash>/)/+", "${slash}");
		final String attrPathNoDot = attrPathRemoveMultipleSeparators.replaceAll("((?<!(\\\\|\\.))\\./(\\.$)?|[^^]/\\.$|(^|(?<=^/))\\.$)", "");
		final String normalizedAttributePath = attrPathNoDot.replaceAll("(?<nonSlash>[^(^|\\\\)])/$", "${nonSlash}");

		final Pattern relativePathPattern = Pattern.compile("[^(/|\\.\\.)]+/\\.\\./?");
		int prevStringLenth = 0;
		String resolvedAttributePath = normalizedAttributePath;
		while (prevStringLenth != resolvedAttributePath.length()) {
			prevStringLenth = resolvedAttributePath.length();
			resolvedAttributePath = relativePathPattern.matcher(resolvedAttributePath).replaceAll("");
		}
		return resolvedAttributePath;
	}

	/**
	 * Encode the inpurt {@link String uri} so that illegal characters are properly escaped prior to generating the resulting {@link URI}.
	 *
	 * @param uri to encode
	 * @return the {@link URI} created from encoding the {@link String uri}
	 * @throws URISyntaxException if {@link String uri} is not valid
	 */
	public static URI encodeAsUri(String uri) throws URISyntaxException {

		if (uri.trim().length() == 0) {
			return new URI(uri);
		}
		/* find last # symbol to split fragment on. If we don't remove it first, then it will encode it, and not parse it separately
		 * after we remove the temporary _N5 scheme */
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
	 * Generate an {@link N5URL} from a container, group, and attribute
	 *
	 * @param container of the N5Url
	 * @param group     of the N5Url
	 * @param attribute of the N5Url
	 * @return the {@link N5URL}
	 * @throws URISyntaxException
	 */
	public static N5URL from(String container, String group, String attribute) throws URISyntaxException {

		final String containerPart = container != null ? container : "";
		final String groupPart = group != null ? "?" + group : "?";
		final String attributePart = attribute != null ? "#" + attribute : "#";
		return new N5URL(containerPart + groupPart + attributePart);
	}
}
