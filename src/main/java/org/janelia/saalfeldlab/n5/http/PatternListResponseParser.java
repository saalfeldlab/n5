package org.janelia.saalfeldlab.n5.http;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link ListResponseParser} that uses a {@link Pattern}.
 *
 * This implementation of parseListResponse returns all the groups named "entry"
 * for all matches of the given pattern.
 */
public class PatternListResponseParser implements ListResponseParser {

	protected Pattern pattern;

	public PatternListResponseParser(Pattern pattern) {

		this.pattern = pattern;
	}

	@Override
	public String[] parseListResponse(final String response) {

		final Matcher matcher = pattern.matcher(response);
		final List<String> matches = new ArrayList<>();
		while (matcher.find()) {
			matches.add(matcher.group("entry"));
		}
		return matches.toArray(new String[0]);
	}

}
