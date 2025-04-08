package org.janelia.saalfeldlab.n5.http;

import java.util.regex.Pattern;

/**
 * {@link ListResponseParser}s for <a href="https://httpd.apache.org/">Apache HTTP Servers</a>.
 */
public abstract class ApacheListResponseParser extends PatternListResponseParser {

	private static final Pattern LIST_ENTRY = Pattern.compile("alt=\"\\[(\\s*|DIR)\\]\".*href=[^>]+>(?<entry>[^<]+)");

	private static final Pattern LIST_DIR_ENTRY = Pattern.compile("alt=\"\\[DIR\\]\".*href=[^>]+>(?<entry>[^<]+)");

	ApacheListResponseParser(Pattern pattern) {

		super(pattern);
	}

	static class ListDirectories extends ApacheListResponseParser {

		public ListDirectories() {

			super(LIST_DIR_ENTRY);
		}
	}

	static class ListAll extends ApacheListResponseParser {

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
