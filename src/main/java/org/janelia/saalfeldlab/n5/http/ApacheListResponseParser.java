/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.http;

import java.util.regex.Pattern;

/**
 * {@link ListResponseParser}s for <a href="https://httpd.apache.org/">Apache HTTP Servers</a>.
 */
abstract class ApacheListResponseParser extends PatternListResponseParser {

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
