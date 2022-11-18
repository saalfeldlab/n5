package org.janelia.saalfeldlab.n5;

import junit.framework.TestCase;

import java.net.URISyntaxException;

public class N5URLTest extends TestCase {

	public void testGetLocation() {

	}

	public void testGetDataset() {

	}

	public void testGetAttribute() {

	}

	public void testGetRelative() throws URISyntaxException {
		assertEquals("/a/b/c/d?e#f", new N5URL("/a/b/c").getRelative("d?e#f").toString());

	}
}