package org.janelia.saalfeldlab.n5.http;

public class CandidateListResponseParser implements ListResponseParser {

	private ListResponseParser[] candidateParsers;

	private ListResponseParser successfulParser;

	public CandidateListResponseParser(ListResponseParser[] candidateParsers) {

		this.candidateParsers = candidateParsers;
	}

	@Override
	public String[] parseListResponse(String response) {

		String[] result = new String[0];
		for (ListResponseParser parser : candidateParsers) {

			result = parser.parseListResponse(response);
			if (result.length > 1) {
				successfulParser = parser;
				return result;
			}
		}

		return result;
	}

	public ListResponseParser successfulParser() {

		return successfulParser;
	}

}
