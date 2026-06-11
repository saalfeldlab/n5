package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


//TODO: Currently, {@code N5Path} is allowed to point outside the root (e.g.,
// {@code "../a/"}), though this is not used internally and should probably be
// explicitly forbidden.

public class N5PathTest {

	@Test
	public void testCreateGroup() {

		// root
		assertEquals("", N5DirectoryPath.of("/").path());
		assertEquals("", N5DirectoryPath.of("").path());
		assertEquals("", N5DirectoryPath.of("a/..").path());
		assertEquals("", N5DirectoryPath.of("a/b/../..").path());

		// trailing slashes (or not)
		assertEquals("a/b/c/", N5DirectoryPath.of("a/b/c").path());
		assertEquals("a/b/c/", N5DirectoryPath.of("a/b/c/").path());
		assertEquals("a/c/", N5DirectoryPath.of("a/b/../c").path());
		assertEquals("a/c/", N5DirectoryPath.of("a/b/../c/").path());

		// leading slashes (or not)
		assertEquals("a/b/", N5DirectoryPath.of("a/b").path());
		assertEquals("a/b/", N5DirectoryPath.of("/a/b").path());
		assertEquals("a/b/", N5DirectoryPath.of("//a/b").path());
		assertEquals("a/b/", N5DirectoryPath.of("///a/b").path());

		// special characters
		assertEquals("my \\group/$a/%b/", N5DirectoryPath.of("my \\group/$a/%b").path());

		// handling of ".." and "."
		assertEquals("a/", N5DirectoryPath.of("a/b/..").path());
		assertEquals("a/", N5DirectoryPath.of("a/././b/..").path());
		assertEquals("b/", N5DirectoryPath.of("a/../b").path());
		assertEquals("../../b/", N5DirectoryPath.of("../../b").path());
	}

	@Test
	public void testCreateFile() {

		assertEquals("a", N5FilePath.of("a").path());
		assertEquals("a/b", N5FilePath.of("a/b").path());

		// root group cannot be treated as a file
		assertThrows(IllegalArgumentException.class, () -> N5FilePath.of(""));
		assertThrows(IllegalArgumentException.class, () -> N5FilePath.of("/"));
		assertThrows(IllegalArgumentException.class, () -> N5FilePath.of("a/.."));

		// strip trailing slashes
		assertEquals("a", N5FilePath.of("a/").path());
		assertEquals("a/b", N5FilePath.of("a/b/").path());

		// strip leading slashes
		assertEquals("a/b", N5FilePath.of("/a/b").path());
		assertEquals("a/b", N5FilePath.of("//a/b").path());
		assertEquals("a/b", N5FilePath.of("///a/b").path());

		// special characters
		assertEquals("my \file/$a/%b", N5FilePath.of("my \file/$a/%b").path());

		// handling of ".." and "."
		assertEquals("b", N5FilePath.of("a/../b").path());
		assertEquals("a/c", N5FilePath.of("a/b/../c").path());
	}

	@Test
	public void testCreate() {

		N5Path p;

		p = N5Path.of("/");
		assertEquals("", p.path());
		assertTrue(p.isDirectory());

		p = N5Path.of("");
		assertEquals("", p.path());
		assertTrue(p.isDirectory());

		p = N5Path.of("a/..");
		assertEquals("", p.path());
		assertTrue(p.isDirectory());

		p = N5Path.of("a/../..");
		assertEquals("../", p.path());
		assertTrue(p.isDirectory());

		p = N5Path.of("a/b/c");
		assertEquals("a/b/c", p.path());
		assertFalse(p.isDirectory());

		p = N5Path.of("a/b/c/");
		assertEquals("a/b/c/", p.path());
		assertTrue(p.isDirectory());

		p = N5Path.of("//a/b");
		assertEquals("a/b", p.path());
		assertFalse(p.isDirectory());

		p = N5Path.of("a/../b");
		assertEquals("b", p.path());
		assertFalse(p.isDirectory());
	}

	@Test
	public void testUri() {

		assertEquals("", N5Path.of("/").uri().toString());
		assertEquals("a", N5Path.of("a").uri().toString());
		assertEquals("a/", N5Path.of("a/").uri().toString());
		assertEquals("a/b", N5Path.of("//a/b").uri().toString());
		assertEquals("my%20%5Cfile/$a/%25b", N5Path.of("my \\file/$a/%b").uri().toString());
	}

	@Test
	public void testNormalPath() {

		assertEquals("", N5Path.of("/").normalPath());
		assertEquals("a", N5Path.of("a").normalPath());
		assertEquals("a", N5Path.of("a/").normalPath());
	}

	@Test
	public void testParent() {

		assertNull(N5Path.of("").parent());
		assertNull(N5Path.of("a/..").parent());

		assertEquals("a/b/", N5Path.of("a/b/c").parent().path());
		assertEquals("a/b/", N5Path.of("a/b/c/").parent().path());
		assertEquals("", N5Path.of("a/b/..").parent().path());
		assertNull(N5Path.of("a/..").parent());
		assertNull(N5Path.of("a/b/../..").parent());
		assertEquals("", N5Path.of("a/../b").parent().path());
		assertEquals("../../", N5Path.of("../../b").parent().path());
		assertEquals("my \file/$a/", N5Path.of("my \file/$a/%b").parent().path());
	}

	@Test
	public void testComponents() {

		assertArrayEquals(new String[] {""}, N5Path.of("").components());
		assertArrayEquals(new String[] {"a"}, N5Path.of("a").components());
		assertArrayEquals(new String[] {"a", "b", "c"}, N5Path.of("a/b/c").components());
		assertArrayEquals(new String[] {"a", "b", "c"}, N5Path.of("a/b/c/").components());
	}
}
