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
package org.janelia.saalfeldlab.n5;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class N5URITest {
	@Test
	public void testAttributePath() {

		assertEquals("/a/b/c/d/e", N5URI.normalizeAttributePath("/a/b/c/d/e"));
		assertEquals("/a/b/c/d/e", N5URI.normalizeAttributePath("/a/b/c/d/e/"));
		assertEquals("/", N5URI.normalizeAttributePath("/"));
		assertEquals("", N5URI.normalizeAttributePath(""));
		assertEquals("/", N5URI.normalizeAttributePath("/a/.."));
		assertEquals("/", N5URI.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URI.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URI.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URI.normalizeAttributePath("/./././././"));
		assertEquals("/", N5URI.normalizeAttributePath("/./././././."));
		assertEquals("", N5URI.normalizeAttributePath("./././././"));
		assertEquals("a.", N5URI.normalizeAttributePath("./a././././"));
		assertEquals("\\\\.", N5URI.normalizeAttributePath("./a./../\\\\././."));
		assertEquals("a./\\\\.", N5URI.normalizeAttributePath("./a./\\\\././."));
		assertEquals("/a./\\\\.", N5URI.normalizeAttributePath("/./a./\\\\././."));

		assertEquals("/a/[0]/b/[0]", N5URI.normalizeAttributePath("/a/[0]/b[0]/"));
		assertEquals("[0]", N5URI.normalizeAttributePath("[0]"));
		assertEquals("/[0]", N5URI.normalizeAttributePath("/[0]/"));
		assertEquals("/a/[0]", N5URI.normalizeAttributePath("/a[0]/"));
		assertEquals("/[0]/b", N5URI.normalizeAttributePath("/[0]b/"));

		assertEquals("[b]", N5URI.normalizeAttributePath("[b]"));
		assertEquals("a[b]c", N5URI.normalizeAttributePath("a[b]c"));
		assertEquals("a[bc", N5URI.normalizeAttributePath("a[bc"));
		assertEquals("ab]c", N5URI.normalizeAttributePath("ab]c"));
		assertEquals("a[b00]c", N5URI.normalizeAttributePath("a[b00]c"));

		assertEquals("[ ]", N5URI.normalizeAttributePath("[ ]"));
		assertEquals("[\n]", N5URI.normalizeAttributePath("[\n]"));
		assertEquals("[\t]", N5URI.normalizeAttributePath("[\t]"));
		assertEquals("[\r\n]", N5URI.normalizeAttributePath("[\r\n]"));
		assertEquals("[ ][\n][\t][ \t \n \r\n][\r\n]", N5URI.normalizeAttributePath("[ ][\n][\t][ \t \n \r\n][\r\n]"));
		assertEquals("[\\]", N5URI.normalizeAttributePath("[\\]"));

		assertEquals("let's/try/a/real/case/with spaces", N5URI.normalizeAttributePath("let's/try/a/real/case/with spaces/"));
		assertEquals("let's/try/a/real/case/with spaces", N5URI.normalizeAttributePath("let's/try/a/real/////case////with spaces/"));
		assertEquals("/first/relative/a/wd/.w/asd", N5URI.normalizeAttributePath("/../first/relative/test/../a/b/.././wd///.w/asd"));
		assertEquals("first/relative/a/wd/.w/asd", N5URI.normalizeAttributePath("../first/relative/test/../a/b/.././wd///.w/asd"));
		assertEquals("", N5URI.normalizeAttributePath("../result/../only/../single/.."));
		assertEquals("", N5URI.normalizeAttributePath("../result/../multiple/../.."));
		assertEquals("/", N5URI.normalizeAttributePath("/../result/../multiple/../.."));

		 assertEquals("b", N5URI.normalizeAttributePath("a/../b"));
		 assertEquals("/b", N5URI.normalizeAttributePath("/a/../b"));
		 assertEquals("b", N5URI.normalizeAttributePath("../a/../b"));
		 assertEquals("/b", N5URI.normalizeAttributePath("/../a/../b"));
		 assertEquals("/b", N5URI.normalizeAttributePath("/../a/../../b"));

		String normalizedPath = N5URI.normalizeAttributePath("let's/try/a/some/////with/ /	//white spaces/");
		assertEquals("Normalizing a normal path should be the identity", normalizedPath, N5URI.normalizeAttributePath(normalizedPath));
	}

	@Test
	public void testEscapedAttributePaths() {

		assertEquals("\\/a\\/b\\/c\\/d\\/e", N5URI.normalizeAttributePath("\\/a\\/b\\/c\\/d\\/e"));
		assertEquals("/a\\\\/b/c", N5URI.normalizeAttributePath("/a\\\\/b/c"));
		assertEquals("a[b]\\[10]", N5URI.normalizeAttributePath("a[b]\\[10]"));
		assertEquals("\\[10]", N5URI.normalizeAttributePath("\\[10]"));
		assertEquals("a/[0]/\\[10]b", N5URI.normalizeAttributePath("a[0]\\[10]b"));
		assertEquals("[\\[10]\\[20]]", N5URI.normalizeAttributePath("[\\[10]\\[20]]"));
		assertEquals("[\\[10]/[20]/]", N5URI.normalizeAttributePath("[\\[10][20]]"));
		assertEquals("\\/", N5URI.normalizeAttributePath("\\/"));
	}

	@Test
	public void testIsAbsolute() throws URISyntaxException {
		/* Always true if scheme provided */
		assertTrue(new N5URI("file:///a/b/c").isAbsolute());
		assertTrue(new N5URI("file://C:\\\\a\\\\b\\\\c").isAbsolute());

		/* Unix Paths*/
		assertTrue(new N5URI("/a/b/c").isAbsolute());
		assertFalse(new N5URI("a/b/c").isAbsolute());

		/* Windows Paths*/
		assertTrue(new N5URI("C:\\\\a\\\\b\\\\c").isAbsolute());
		assertFalse(new N5URI("a\\\\b\\\\c").isAbsolute());
	}

	@Test
	public void testGetRelative() throws URISyntaxException {

		assertEquals(
				"/a/b/c/d?e#f",
				new N5URI("/a/b/c").resolve("d?e#f").toString());
		assertEquals(
				"/d?e#f",
				new N5URI("/a/b/c").resolve("/d?e#f").toString());
		assertEquals(
				"file:/a/b/c",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("file:/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c?d/e#f/g",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("/a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5/a/b/c?d/e#f/g",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5?d/e#f/g",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5#f/g",
				new N5URI("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("#f/g").toString());
	}
}
