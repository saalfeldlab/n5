package org.janelia.saalfeldlab.n5.http;

import java.util.regex.Pattern;

/**
 * {@link ListResponseParser}s for <a href="https://docs.python.org/3/library/http.server.html">Python's SimpleHTTP Server</a>.
 */
public abstract class PythonListResponseParser extends PatternListResponseParser {

	private static final Pattern LIST_ENTRY = Pattern.compile("href=\"[^\"]+\">(?<entry>[^<]+)");

	private static final Pattern LIST_DIR_ENTRY = Pattern.compile("href=\"[^\"]+\">(?<entry>[^<]+)/");

	PythonListResponseParser(Pattern pattern) {

		super(pattern);
	}

	static class ListDirectories extends PythonListResponseParser {

		public ListDirectories() {

			super(LIST_DIR_ENTRY);
		}
	}

	static class ListAll extends PythonListResponseParser {

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
