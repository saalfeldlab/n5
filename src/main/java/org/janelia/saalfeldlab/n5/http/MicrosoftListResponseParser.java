package org.janelia.saalfeldlab.n5.http;

import java.util.regex.Pattern;

/**
 * {@link ListResponseParser}s for <a href="https://www.iis.net/">Microsoft-IIS Servers</a>.
 */
abstract class MicrosoftListResponseParser extends PatternListResponseParser {

	private static final Pattern LIST_ENTRY = Pattern.compile( "HREF=[^>]+>(?!\\[To Parent Directory])(?<entry>[^<]+)");

	private static final Pattern LIST_DIR_ENTRY = Pattern.compile( "&lt;dir&gt;[^H]+HREF=[^>]+>(?!\\[To Parent Directory])(?<entry>[^<]+)");

	MicrosoftListResponseParser(Pattern pattern) {

		super(pattern);
	}

	static class ListDirectories extends MicrosoftListResponseParser {

		public ListDirectories() {

			super(LIST_DIR_ENTRY);
		}
	}

	static class ListAll extends MicrosoftListResponseParser {

		public ListAll() {

			super(LIST_ENTRY);
		}
	}

	public static ListResponseParser directoryParser() {

		return new ListDirectories();
	}

	public static ListResponseParser parser() {

		return new ListAll();
	}

}
