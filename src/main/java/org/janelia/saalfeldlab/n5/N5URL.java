package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;

public class N5URL {

	final URI uri;
	private String scheme;
	private String location;
	private String dataset;
	private String attribute;

	public N5URL(String uri) throws URISyntaxException {
		this(encodeAsUri(uri));
	}

	public N5URL(URI uri) {

		this.uri = uri;
		final int queryIdx = uri.getSchemeSpecificPart().indexOf('?');
		final String schemeSpecificPartWithoutQuery = uri.getSchemeSpecificPart().substring(0, queryIdx);
		scheme = uri.getScheme() == null ? null : uri.getScheme();
		if (uri.getScheme() == null) {
			location = schemeSpecificPartWithoutQuery.replaceFirst("//", "");
		} else {
			location = uri.getScheme() + ":" + schemeSpecificPartWithoutQuery;
		}
		if (queryIdx > 0) {
			dataset = uri.getSchemeSpecificPart().substring(queryIdx + 1);
		} else {
			dataset = null;
		}
		attribute = uri.getFragment();
	}

	@Override public String toString() {

		String scheme = "";
		if (uri.getScheme() != null) {
			scheme = uri.getScheme() + ":";
		}
		String fragment = "";
		if (uri.getFragment() != null) {
			fragment = "#" + uri.getFragment();
		}
		return scheme + uri.getSchemeSpecificPart() + fragment;
	}

	public N5URL getRelative(URI relative) {
		return null;
	}

	public static URI encodeAsUri(String uri) throws URISyntaxException {

		/* find last # symbol to split fragment on. If we don't remove it first, then it will encode it, and not parse it separately
		* after we remove the temporary _N5 scheme */

		final int  fragmentIdx  = uri.lastIndexOf('#');
		final String uriWithoutFragment;
		final String fragment;
		if (fragmentIdx > 0) {
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

//		System.out.println(new N5URL("/this/is/a/test?dataset#/a/b/attributes[0]"));
		System.out.println(new N5URL("s3://janelia-co 1234 5t 56 sem-datasets/jrc_ma 445t 6crophage-2/jrc_macr 2d3 464 ophage-2.n5?d2 3d?aset#a d245 sdf"));
	}

}
