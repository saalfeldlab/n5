package org.janelia.saalfeldlab.n5;

import org.junit.Test;

import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class N5URLTest {
	@Test
	public void testAttributePath() {
			assertEquals("/a/b/c/d/e", N5URL.resolveAttributePath("/a/b/c/d/e"));
			assertEquals("/a/b/c/d/e", N5URL.resolveAttributePath("/a/b/c/d/e/"));
			assertEquals("/", N5URL.resolveAttributePath("/"));
			assertEquals("", N5URL.resolveAttributePath(""));
			assertEquals("/", N5URL.resolveAttributePath("/a/.."));
			assertEquals("/", N5URL.resolveAttributePath("/a/../b/../c/d/../.."));
			assertEquals("/", N5URL.resolveAttributePath("/a/../b/../c/d/../.."));
			assertEquals("/", N5URL.resolveAttributePath("/a/../b/../c/d/../.."));
			assertEquals("/", N5URL.resolveAttributePath("/./././././"));
			assertEquals("", N5URL.resolveAttributePath("./././././"));

			assertEquals("/a/[0]/b/[0]", N5URL.resolveAttributePath("/a/[0]/b[0]/"));
			assertEquals("[0]", N5URL.resolveAttributePath("[0]"));
			assertEquals("/[0]", N5URL.resolveAttributePath("/[0]/"));
			assertEquals("/a/[0]", N5URL.resolveAttributePath("/a[0]/"));
			assertEquals("/[0]/b", N5URL.resolveAttributePath("/[0]b/"));

			assertEquals("let's/try/a/real/case/with spaces", N5URL.resolveAttributePath("let's/try/a/real/case/with spaces/"));
			assertEquals("let's/try/a/real/case/with spaces", N5URL.resolveAttributePath("let's/try/a/real/////case////with spaces/"));
			assertThrows( IndexOutOfBoundsException.class, () -> N5URL.resolveAttributePath("../first/relative/../not/allowed"));
	}

	@Test
	public void testGetRelative() throws URISyntaxException {

		assertEquals(
				"/a/b/c/d?e#f",
				new N5URL("/a/b/c").getRelative("d?e#f").toString());
		assertEquals(
				"/d?e#f",
				new N5URL("/a/b/c").getRelative("/d?e#f").toString());
		assertEquals(
				"file:/a/b/c",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("file:/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("/a/b/c").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/a/b/c?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("/a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5/a/b/c?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("a/b/c?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5?d/e#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("?d/e#f/g").toString());
		assertEquals(
				"s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5#f/g",
				new N5URL("s3://janelia-cosem-datasets/jrc_hela-3/jrc_hela-3.n5").getRelative("#f/g").toString());
	}
}