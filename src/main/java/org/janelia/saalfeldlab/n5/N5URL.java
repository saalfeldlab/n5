package org.janelia.saalfeldlab.n5;

import org.apache.commons.compress.compressors.FileNameUtil;
import org.apache.commons.compress.utils.FileNameUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

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

	public String getAttribute() {

		return attribute;
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

		if (relativeUri.getPath() != null) {
			if (!relativeUri.getPath().startsWith("/")) {
				newUri.append(thisUri.getPath()).append('/');
			}
			newUri
					.append(relativeUri.getPath())
					.append(relative.getDatasetPart())
					.append(relative.getAttributePart());
			return new N5URL(newUri.toString());
		}
		newUri.append(thisUri.getPath());

		if (relativeUri.getQuery() != null) {
			newUri
					.append(relative.getDatasetPart())
					.append(relative.getAttributePart());
			return new N5URL(newUri.toString());
		}
		newUri.append(this.getDatasetPart());

		if (relativeUri.getFragment() != null) {
			newUri.append(relative.getAttributePart());
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

	public static URI encodeAsUri(String uri) throws URISyntaxException {

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
		final URI _n5Uri = new URI("N5Internal", uriWithoutFragment, fragment);

		final URI n5Uri;
		if (fragment == null) {
			n5Uri = new URI(_n5Uri.getRawSchemeSpecificPart());
		} else {
			n5Uri = new URI(_n5Uri.getRawSchemeSpecificPart() + "#" + _n5Uri.getRawFragment());
		}
		return n5Uri;
	}

	public static void main(String[] args) throws URISyntaxException {

		new N5URL("/a/b/c").getRelative("d?a#test");
	}

}
