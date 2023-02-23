package org.janelia.saalfeldlab.n5;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class N5URLTest {
	@Test
	public void testAttributePath() {

		assertEquals("/a/b/c/d/e", N5URL.normalizeAttributePath("/a/b/c/d/e"));
		assertEquals("/a/b/c/d/e", N5URL.normalizeAttributePath("/a/b/c/d/e/"));
		assertEquals("/", N5URL.normalizeAttributePath("/"));
		assertEquals("", N5URL.normalizeAttributePath(""));
		assertEquals("/", N5URL.normalizeAttributePath("/a/.."));
		assertEquals("/", N5URL.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URL.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URL.normalizeAttributePath("/a/../b/../c/d/../.."));
		assertEquals("/", N5URL.normalizeAttributePath("/./././././"));
		assertEquals("/", N5URL.normalizeAttributePath("/./././././."));
		assertEquals("", N5URL.normalizeAttributePath("./././././"));
		assertEquals("a.", N5URL.normalizeAttributePath("./a././././"));
		assertEquals("\\\\.", N5URL.normalizeAttributePath("./a./../\\\\././."));
		assertEquals("a./\\\\.", N5URL.normalizeAttributePath("./a./\\\\././."));
		assertEquals("/a./\\\\.", N5URL.normalizeAttributePath("/./a./\\\\././."));

		assertEquals("/a/[0]/b/[0]", N5URL.normalizeAttributePath("/a/[0]/b[0]/"));
		assertEquals("[0]", N5URL.normalizeAttributePath("[0]"));
		assertEquals("/[0]", N5URL.normalizeAttributePath("/[0]/"));
		assertEquals("/a/[0]", N5URL.normalizeAttributePath("/a[0]/"));
		assertEquals("/[0]/b", N5URL.normalizeAttributePath("/[0]b/"));

		assertEquals("[b]", N5URL.normalizeAttributePath("[b]"));
		assertEquals("a[b]c", N5URL.normalizeAttributePath("a[b]c"));
		assertEquals("a[bc", N5URL.normalizeAttributePath("a[bc"));
		assertEquals("ab]c", N5URL.normalizeAttributePath("ab]c"));
		assertEquals("a[b00]c", N5URL.normalizeAttributePath("a[b00]c"));

		assertEquals("[ ]", N5URL.normalizeAttributePath("[ ]"));
		assertEquals("[\n]", N5URL.normalizeAttributePath("[\n]"));
		assertEquals("[\t]", N5URL.normalizeAttributePath("[\t]"));
		assertEquals("[\r\n]", N5URL.normalizeAttributePath("[\r\n]"));
		assertEquals("[ ][\n][\t][ \t \n \r\n][\r\n]", N5URL.normalizeAttributePath("[ ][\n][\t][ \t \n \r\n][\r\n]"));
		assertEquals("[\\]", N5URL.normalizeAttributePath("[\\]"));

		assertEquals("let's/try/a/real/case/with spaces", N5URL.normalizeAttributePath("let's/try/a/real/case/with spaces/"));
		assertEquals("let's/try/a/real/case/with spaces", N5URL.normalizeAttributePath("let's/try/a/real/////case////with spaces/"));
		assertEquals("../first/relative/a/wd/.w/asd", N5URL.normalizeAttributePath("../first/relative/test/../a/b/.././wd///.w/asd"));
		assertEquals("../", N5URL.normalizeAttributePath("../result/../only/../single/.."));
		assertEquals("../..", N5URL.normalizeAttributePath("../result/../multiple/../.."));

		String normalizedPath = N5URL.normalizeAttributePath("let's/try/a/some/////with/ /	//white spaces/");
		assertEquals("Normalizing a normal path should be the identity", normalizedPath, N5URL.normalizeAttributePath(normalizedPath));
	}

	@Test
	public void testEscapedAttributePaths() {

		assertEquals("\\/a\\/b\\/c\\/d\\/e", N5URL.normalizeAttributePath("\\/a\\/b\\/c\\/d\\/e"));
		assertEquals("/a\\\\/b/c", N5URL.normalizeAttributePath("/a\\\\/b/c"));
		assertEquals("a[b]\\[10]", N5URL.normalizeAttributePath("a[b]\\[10]"));
		assertEquals("\\[10]", N5URL.normalizeAttributePath("\\[10]"));
		assertEquals("a/[0]/\\[10]b", N5URL.normalizeAttributePath("a[0]\\[10]b"));
		assertEquals("[\\[10]\\[20]]", N5URL.normalizeAttributePath("[\\[10]\\[20]]"));
		assertEquals("[\\[10]/[20]/]", N5URL.normalizeAttributePath("[\\[10][20]]"));
		assertEquals("\\/", N5URL.normalizeAttributePath("\\/"));
	}

	@Test
	public void testIsAbsolute() throws URISyntaxException {
		/* Always true if scheme provided */
		assertTrue(new N5URL("file:///a/b/c").isAbsolute());
		assertTrue(new N5URL("file://C:\\\\a\\\\b\\\\c").isAbsolute());

		/* Unix Paths*/
		assertTrue(new N5URL("/a/b/c").isAbsolute());
		assertFalse(new N5URL("a/b/c").isAbsolute());

		/* Windows Paths*/
		assertTrue(new N5URL("C:\\\\a\\\\b\\\\c").isAbsolute());
		assertFalse(new N5URL("a\\\\b\\\\c").isAbsolute());
	}

	@Test
	public void testGetRelative() throws URISyntaxException {

		//TODO: nio path interface?
		// Path
		assertEquals(
				"/a/b/c/d?e#f",
				new N5URL("/a/b/c").resolve("d?e#f").toString());
		assertEquals(
				"/d?e#f",
				new N5URL("/a/b/c").resolve("/d?e#f").toString());
		assertEquals(
				"file:/a/b/c",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("file:/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("/a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5/a/b/c?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").resolve("#f/g").toString());
	}
}