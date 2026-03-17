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

import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
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
		assertEquals("", N5GroupPath.of("/").path());
		assertEquals("", N5GroupPath.of("").path());
		assertEquals("", N5GroupPath.of("a/..").path());
		assertEquals("", N5GroupPath.of("a/b/../..").path());

		// trailing slashes (or not)
		assertEquals("a/b/c/", N5GroupPath.of("a/b/c").path());
		assertEquals("a/b/c/", N5GroupPath.of("a/b/c/").path());
		assertEquals("a/c/", N5GroupPath.of("a/b/../c").path());
		assertEquals("a/c/", N5GroupPath.of("a/b/../c/").path());

		// leading slashes (or not)
		assertEquals("a/b/", N5GroupPath.of("a/b").path());
		assertEquals("a/b/", N5GroupPath.of("/a/b").path());
		assertEquals("a/b/", N5GroupPath.of("//a/b").path());
		assertEquals("a/b/", N5GroupPath.of("///a/b").path());

		// special characters
		assertEquals("my \\group/$a/%b/", N5GroupPath.of("my \\group/$a/%b").path());

		// handling of ".." and "."
		assertEquals("a/", N5GroupPath.of("a/b/..").path());
		assertEquals("a/", N5GroupPath.of("a/././b/..").path());
		assertEquals("b/", N5GroupPath.of("a/../b").path());
		assertEquals("../../b/", N5GroupPath.of("../../b").path());
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
		assertTrue(p.isGroup());

		p = N5Path.of("");
		assertEquals("", p.path());
		assertTrue(p.isGroup());

		p = N5Path.of("a/..");
		assertEquals("", p.path());
		assertTrue(p.isGroup());

		p = N5Path.of("a/../..");
		assertEquals("../", p.path());
		assertTrue(p.isGroup());

		p = N5Path.of("a/b/c");
		assertEquals("a/b/c", p.path());
		assertFalse(p.isGroup());

		p = N5Path.of("a/b/c/");
		assertEquals("a/b/c/", p.path());
		assertTrue(p.isGroup());

		p = N5Path.of("//a/b");
		assertEquals("a/b", p.path());
		assertFalse(p.isGroup());

		p = N5Path.of("a/../b");
		assertEquals("b", p.path());
		assertFalse(p.isGroup());
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
