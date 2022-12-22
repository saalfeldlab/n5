package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import com.google.gson.JsonPrimitive;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
	 *
	 * Parse this {@link N5URL} as a list of {@link N5UrlAttributePathToken}.
	 *
	 * @see N5URL#getAttributePathTokens(Gson, String, Object)
	 */
	public ArrayList<N5UrlAttributePathToken> getAttributePathTokens(Gson gson) {
		return getAttributePathTokens(gson, null);
	}

	/**
	 *
	 * Parse this {@link N5URL} as a list of {@link N5UrlAttributePathToken}.
	 *
	 * @see N5URL#getAttributePathTokens(Gson, String, Object)
	 */
	public <T> ArrayList<N5UrlAttributePathToken> getAttributePathTokens(Gson gson, T value) {

		return N5URL.getAttributePathTokens(gson, normalizeAttributePath(), value);
	}

	/**
	 * Parses the {@link String normalizedAttributePath} to a list of {@link N5UrlAttributePathToken}.
	 * This is useful for traversing or constructing a json representation of the provided {@link String normalizedAttributePath}.
	 * Note that {@link String normalizedAttributePath} should be normalized prior to generating this list
	 *
	 * @param gson used to parse {@link T value}
	 * @param normalizedAttributePath to parse into {@link N5UrlAttributePathToken N5UrlAttributePathTokens}
	 * @param value to be stored in the last {@link N5UrlAttributePathToken}
	 * @param <T> type of the value representing the last {@link N5UrlAttributePathToken}
	 * @return a list of {@link N5UrlAttributePathToken N5AttributePathTokens}
	 */
	public static <T> ArrayList<N5UrlAttributePathToken> getAttributePathTokens(Gson gson, String normalizedAttributePath, T value) {

		final String[] attributePaths = normalizedAttributePath.replaceAll("^/", "").split("/");

		if (attributePaths.length == 0) {
			return new ArrayList<>();
		} else if ( attributePaths.length == 1 && attributePaths[0].isEmpty()) {
			final N5UrlAttributePathLeaf<?> leaf = new N5UrlAttributePathLeaf<>(gson, value);
			final ArrayList<N5UrlAttributePathToken> tokens = new ArrayList<>();
			tokens.add(leaf);
			return tokens;
		}
		final ArrayList<N5UrlAttributePathToken> n5UrlAttributePathTokens = new ArrayList<>();
		for (int i = 0; i < attributePaths.length; i++) {
			final String pathToken = attributePaths[i];
			final Matcher matcher = ARRAY_INDEX.matcher(pathToken);
			final N5UrlAttributePathToken newToken;
			if (matcher.matches()) {
				final int index = Integer.parseInt(matcher.group().replace("[", "").replace("]", ""));
				newToken = new N5UrlAttributePathArray(gson, index);
				n5UrlAttributePathTokens.add(newToken);
			} else {
				newToken = new N5UrlAttributePathObject(gson, pathToken);
				n5UrlAttributePathTokens.add(newToken);
			}

			if (i > 0) {
				final N5UrlAttributePathToken parent = n5UrlAttributePathTokens.get(i - 1);
				parent.setChild(newToken);
				newToken.setParent(parent);
			}
		}

		if (value != null) {
			final N5UrlAttributePathLeaf<?> leaf = new N5UrlAttributePathLeaf<>(gson, value);
			/* get last token, if present */
			if (n5UrlAttributePathTokens.size() >= 1) {
				final N5UrlAttributePathToken lastToken = n5UrlAttributePathTokens.get(n5UrlAttributePathTokens.size() - 1);
				lastToken.setChild(leaf);
				leaf.setParent(lastToken);
			}
			n5UrlAttributePathTokens.add(leaf);
		}
		return n5UrlAttributePathTokens;
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

		return uri.getSchemeSpecificPart().replace("?" + uri.getQuery(), "");
	}

	/**
	 * N5URL is always considered absolute if a scheme is provided.
	 * If no scheme is provided, the N5URL is absolute if it starts with either "/" or "[A-Z]:"
	 *
	 * @return if the path for this N5URL is absolute
	 */
	public boolean isAbsolute() {

		if (scheme != null) return true;
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
	 *<p>
	 * Attribute paths have a few of special characters:
	 * <ul>
	 * 	<li>"." which represents the current element </li>
	 * 	<li>".." which represent the previous elemnt </li>
	 * 	<li>"/" which is used to separate elements in the json tree </li>
	 * 	<li>[N] where N is an integer, refer to an index in the previous element in the tree; the previous element must be an array. </li>
	 * 		Note: [N] also separates the previous and following elements, regardless of whether it is preceded by "/" or not.
	 * </ul>
	 * 	<p>
	 * When normalizing:
	 * <ul>
	 * 	<li>"/" are added before and after any indexing brackets [N] </li>
	 * 	<li>any redundant "/" are removed </li>
	 * 	<li>any relative ".." and "." are resolved </li>
	 * </ul>
	 *
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

		final char[] pathChars = attributePath.toCharArray();

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
			} else if (character == '/' || character == '[' || character == ']') {
				if (character == '/' && tokens.isEmpty() && curToken.length() == 0) {
					/* If we are root, and the first token, then add the '/' */
					curToken.append(character);
				} else if (character == ']') {
					/* If ']' add before terminating the token */
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

				/* if '[' add to the start of the next token */
				if (character == '[') {
					curToken.append('[');
				}
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
	 * Encode the inpurt {@link String uri} so that illegal characters are properly escaped prior to generating the resulting {@link URI}.
	 *
	 * @param uri to encode
	 *
	 * @return the {@link URI} created from encoding the {@link String uri}
	 *
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
	public static abstract class N5UrlAttributePathToken {

		protected final Gson gson;

		protected JsonElement root;

		protected N5UrlAttributePathToken parent;
		protected N5UrlAttributePathToken child;

		public N5UrlAttributePathToken(Gson gson) {

			this.gson = gson;
		}

		public void setChild(N5UrlAttributePathToken child) {

			this.child = child;
		}

		public void setParent(N5UrlAttributePathToken parent) {

			this.parent = parent;
		}

		public JsonElement getRoot() {

			return this.root;
		}

		public abstract boolean canNavigate(JsonElement json);

		public abstract JsonElement navigateJsonElement(JsonElement json);

		public JsonElement replaceJsonElement(JsonElement jsonParent) {

			parent.removeJsonElement(jsonParent);
			return parent.createJsonElement(jsonParent);
		}

		public abstract JsonElement createJsonElement(JsonElement json);

		public abstract void writeLeaf(JsonElement json);

		protected abstract void removeJsonElement(JsonElement jsonParent);

		public abstract boolean jsonIncompatible(JsonElement json);
	}

	public static class N5UrlAttributePathObject extends N5UrlAttributePathToken {

		public final String key;

		public N5UrlAttributePathObject(Gson gson, String key) {

			super(gson);
			this.key = key;
		}

		public String getKey()
		{
			return key;
		}

		@Override public boolean jsonIncompatible(JsonElement json) {

			return json != null && !json.isJsonObject();
		}

		@Override public boolean canNavigate(JsonElement json) {
			if (json == null) {
				return false;
			}
			return json.isJsonObject() && json.getAsJsonObject().has(key);
		}

		@Override public JsonElement navigateJsonElement(JsonElement json) {

			return json.getAsJsonObject().get(key);
		}

		@Override protected void removeJsonElement(JsonElement jsonParent) {
			jsonParent.getAsJsonObject().remove(key);
		}

		@Override public JsonElement createJsonElement(JsonElement json) {
			final JsonObject object;
			if (json == null) {
				object = new JsonObject();
				root = object;
			} else {
				object = json.getAsJsonObject();
			}

			final JsonElement newJsonElement;
			if (child instanceof N5UrlAttributePathArray) {
				newJsonElement = new JsonArray();
				object.add(key, newJsonElement);
			} else if (child instanceof N5UrlAttributePathObject) {
				newJsonElement = new JsonObject();
				object.add(key, newJsonElement);
			} else {
				newJsonElement = new JsonObject();
				writeLeaf(object);
			}
			return newJsonElement;
		}

		@Override public void writeLeaf(JsonElement json) {

			final N5UrlAttributePathLeaf<?> leaf = ((N5UrlAttributePathLeaf<?>)child);
			final JsonElement leafJson = leaf.createJsonElement(null);
			json.getAsJsonObject().add(key, leafJson);
		}
	}

	public static class N5UrlAttributePathArray extends N5UrlAttributePathToken {

		@Override public boolean jsonIncompatible(JsonElement json) {

			return json!= null && !json.isJsonArray();
		}

		@Override protected void removeJsonElement(JsonElement jsonParent) {
			jsonParent.getAsJsonArray().set(index, JsonNull.INSTANCE);
		}

		private final int index;

		public N5UrlAttributePathArray(Gson gson, int index) {

			super(gson);
			this.index = index;
		}

		@Override public boolean canNavigate(JsonElement json) {
			if (json == null) {
				return false;
			}

			return json.isJsonArray() && json.getAsJsonArray().size() > index;
		}

		@Override public JsonElement navigateJsonElement(JsonElement json) {

			return json.getAsJsonArray().get(index);
		}

		@Override public JsonElement createJsonElement(JsonElement json) {

			final JsonArray array;
			if (json == null) {
				array = new JsonArray();
				root = array;
			} else {
				array = json.getAsJsonArray();
			}

			JsonElement fillArrayValue = JsonNull.INSTANCE;

			if (child instanceof N5UrlAttributePathLeaf<?>) {
				final N5UrlAttributePathLeaf< ? > leaf = ( N5UrlAttributePathLeaf< ? > ) child;
				if (leaf.value instanceof Number || (leaf.value instanceof JsonPrimitive && ((JsonPrimitive)leaf.value).isNumber())) {
					fillArrayValue = new JsonPrimitive( 0 );
				}
			}

			for (int i = array.size(); i <= index; i++) {
				array.add(fillArrayValue);
			}
			final JsonElement newJsonElement;
			if (child instanceof N5UrlAttributePathArray) {
				newJsonElement = new JsonArray();
				array.set(index, newJsonElement);
			} else if (child instanceof N5UrlAttributePathObject) {
				newJsonElement = new JsonObject();
				array.set(index, newJsonElement);
			} else {
				newJsonElement = child.createJsonElement(null);
				array.set(index, newJsonElement);
			}
			return newJsonElement;
		}

		@Override public void writeLeaf(JsonElement json) {

			final N5UrlAttributePathLeaf<?> leaf = ((N5UrlAttributePathLeaf<?>)child);
			final JsonElement leafJson = leaf.createJsonElement(null);
			final JsonArray array = json.getAsJsonArray();

			for (int i = array.size(); i <= index; i++) {
				array.add(JsonNull.INSTANCE);
			}

			array.set(index, leafJson);
		}
	}

	public static class N5UrlAttributePathLeaf<T> extends N5UrlAttributePathToken {

		private final T value;

		public N5UrlAttributePathLeaf(Gson gson, T value) {

			super(gson);
			this.value = value;
		}

		@Override public boolean canNavigate(JsonElement json) {

			return false;
		}

		@Override public boolean jsonIncompatible(JsonElement json) {

			return false;
		}

		@Override public JsonElement navigateJsonElement(JsonElement json) {

			throw new UnsupportedOperationException("Cannot Navigate from a leaf ");
		}

		@Override protected void removeJsonElement(JsonElement jsonParent) {
			throw new UnsupportedOperationException("Leaf Removal Unnecessary, leaves should alway be overwritable");

		}

		@Override public JsonElement createJsonElement(JsonElement json) {
			/* Leaf is weird. It is never added manually, also via the previous path token.
			* HOWEVER, when there is no path token, and the leaf is the root, than it is called manually,
			* and we wish to be our own parent (i.e. the root of the json tree). */
			final JsonElement leaf = gson.toJsonTree(value);
			if (json == null) {
				root = leaf;
			}
			return leaf;
		}

		@Override public void writeLeaf(JsonElement json) {
			/* Practically, this method doesn't mean much when you are a leaf.
			* It's primarily meant to have the parent write their children, if their child is a leaf.  */
		}


	}

	/**
	 * Generate an {@link N5URL} from a container, group, and attribute
	 *
	 * @param container of the N5Url
	 * @param group of the N5Url
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
