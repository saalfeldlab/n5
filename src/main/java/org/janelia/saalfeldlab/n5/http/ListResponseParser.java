package org.janelia.saalfeldlab.n5.http;

/**
 * A
 */
public interface ListResponseParser {

	/**
	 * Parse a String response for a list call, and return the results as a
	 * String array.
	 *
	 * @param response
	 *            an (http) response
	 * @return the list elements it contains
	 */
	String[] parseListResponse(final String response);
}
