package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class N5URL {

	final URI uri;
	private final String scheme;
	private final String location;
	private final String dataset;
	private final String attribute;

	public N5URL(String uri) throws URISyntaxException {

		this(encodeAsUri(uri));
	}

	public N5URL(URI uri) {

		this.uri = uri;
		scheme = uri.getScheme() == null ? null : uri.getScheme();
		final String schemeSpecificPartWithoutQuery = getSchemeSpecificPartWithoutQuery();
		if (uri.getScheme() == null) {
			location = schemeSpecificPartWithoutQuery.replaceFirst("//", "");
		} else {
			location = uri.getScheme() + ":" + schemeSpecificPartWithoutQuery;
		}
		dataset = uri.getQuery();
		attribute = uri.getFragment();
	}

	public String getLocation() {

		return location;
	}

	public String getDataset() {

		return dataset;
	}

	public String resolveDataset() {

		return resolveDatasetPath( getDataset() == null ? "" : getDataset() );
	}

	public String getAttribute() {

		return attribute;
	}

	public String resolveAttribute() {

		return resolveAttributePath( getAttribute() == null ? "" : getAttribute() );
	}

	private String getSchemePart() {

		return scheme == null ? "" : scheme + "://";
	}

	private String getLocationPart() {

		return location;
	}

	private String getDatasetPart() {

		return dataset == null ? "" : "?" + dataset;
	}

	private String getAttributePart() {

		return attribute == null ? "" : "#" + attribute;
	}

	@Override public String toString() {

		return getLocationPart() + getDatasetPart() + getAttributePart();
	}

	private String getSchemeSpecificPartWithoutQuery() {

		return uri.getSchemeSpecificPart().replace("?" + uri.getQuery(), "");
	}

	public N5URL getRelative(N5URL relative) throws URISyntaxException {

		final URI thisUri = uri;
		final URI relativeUri = relative.uri;

		final StringBuilder newUri = new StringBuilder();

		if (relativeUri.getScheme() != null) {
			return relative;
		}
		final String thisScheme = thisUri.getScheme();
		if (thisScheme != null) {
			newUri.append(thisScheme).append(":");
		}

		if (relativeUri.getAuthority() != null) {
			newUri
					.append(relativeUri.getAuthority())
					.append(relativeUri.getPath())
					.append(relative.getDatasetPart())
					.append(relative.getAttributePart());
			return new N5URL(newUri.toString());
		}
		final String thisAuthority = thisUri.getAuthority();
		if (thisAuthority != null) {
			newUri.append("//").append(thisAuthority);
		}

		if (!relativeUri.getPath().isEmpty()) {
			final String path = relativeUri.getPath();
			final char char0 = path.charAt(0);
			final boolean isAbsolute = char0 == '/' || (path.length() >= 2 && path.charAt(1) == ':' && char0 >= 'A' && char0 <= 'Z');
			if (!isAbsolute) {
				newUri.append(thisUri.getPath()).append('/');
			}
			newUri
					.append(path)
					.append(relative.getDatasetPart())
					.append(relative.getAttributePart());
			return new N5URL(newUri.toString());
		}
		newUri.append(thisUri.getPath());

		final String query = relativeUri.getQuery();
		if (query != null) {
			if (query.charAt(0) != '/' && thisUri.getQuery() != null) {
				newUri.append(this.getDatasetPart()).append('/');
				newUri.append(relativeUri.getQuery());
			} else {
				newUri.append(relative.getDatasetPart());
			}
			newUri.append(relative.getAttributePart());
			return new N5URL(newUri.toString());
		}
		newUri.append(this.getDatasetPart());

		final String fragment = relativeUri.getFragment();
		if (fragment != null) {
			if (fragment.charAt(0) != '/' && thisUri.getFragment() != null) {
				newUri.append(this.getAttributePart()).append('/');
			} else {
				newUri.append(relative.getAttributePart());
			}

			return new N5URL(newUri.toString());
		}
		newUri.append(this.getAttributePart());

		return new N5URL(newUri.toString());
	}

	public N5URL getRelative(URI relative) throws URISyntaxException {

		return getRelative(new N5URL(relative));
	}

	public N5URL getRelative(String relative) throws URISyntaxException {

		return getRelative(new N5URL(relative));
	}

	public static String resolveDatasetPath(String path) {
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
		if ( !lastToken.isEmpty() ) {
			if (lastToken.equals( ".." )) {
				tokens.remove( tokens.size() - 1 );
			} else {
				tokens.add( lastToken );
			}
		}
		if (tokens.isEmpty()) return "";
		String root = "";
		if (tokens.get(0).equals("/")) {
			tokens.remove(0);
			root = "/";
		}
		return root + tokens.stream()
				.filter(it -> !it.equals("."))
				.filter(it ->!it.isEmpty())
				.reduce((l,r) -> l + "/" + r).orElse( "" );
	}

	public static String resolveAttributePath(String path) {
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
			} else if (character == '/' || character == '[' || character == ']') {
				if (character == '/' && tokens.isEmpty() && curToken.length() == 0) {
					/* If we are root, and the first token, then add the '/' */
					curToken.append(character);
				} else  if (character == ']') {
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
		if ( !lastToken.isEmpty() ) {
			if (lastToken.equals( ".." )) {
				tokens.remove( tokens.size() - 1 );
			} else {
				tokens.add( lastToken );
			}
		}
		if (tokens.isEmpty()) return "";
		String root = "";
		if (tokens.get(0).equals("/")) {
			tokens.remove(0);
			root = "/";
		}
		return root + tokens.stream()
				.filter(it -> !it.equals("."))
				.filter(it ->!it.isEmpty())
				.reduce((l,r) -> l + "/" + r).orElse( "" );
	}

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
		if (uriWithoutFragment.length() == 0 && fragment != null && fragment.length() > 0 ) {
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

	public static N5URL from(String container, String group, String attribute) throws URISyntaxException {
		final String containerPart = container != null ? container : "";
		final String groupPart = group != null ? "?" + group : "?";
		final String attributePart = attribute != null ? "#" + attribute : "#";
		return new N5URL(containerPart + groupPart + attributePart);
	}
}
