package org.janelia.saalfeldlab.n5.http;

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

	public static ListResponseParser defaultListParser() {

		return new CandidateListResponseParser(
				new ListResponseParser[]{
						MicrosoftListResponseParser.parser(),
						ApacheListResponseParser.parser(),
						PythonListResponseParser.parser()
				});
	}

	public static ListResponseParser defaultDirectoryListParser() {

		return new CandidateListResponseParser(
				new ListResponseParser[]{
						MicrosoftListResponseParser.directoryParser(),
						ApacheListResponseParser.directoryParser(),
						PythonListResponseParser.directoryParser()
				});
	}

}
