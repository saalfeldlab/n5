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
package org.janelia.saalfeldlab.n5.backward;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.junit.Test;

import com.google.gson.JsonElement;

public class CompatibilityTest {

	String[][] readVersionsDataset = {
			{"data-1.5.0.n5", "raw"},
			{"data-2.5.1.n5", "raw"},
			{"data-3.1.3.n5", "raw"} };

	String writeVersion = "data-3.1.3.n5";
	String writeDataset = "raw";
	String[] writePathsToTest = {"0/0", "0/1", "1/0", "1/1"};

	@Test
	public void testBackwardReads() throws NumberFormatException, IOException {

		for (String[] versionDset : readVersionsDataset)
			backwardReadHelper(versionDset[0], versionDset[1]);
	}

	public void backwardReadHelper(final String base, final String dsetPath) throws NumberFormatException, IOException {

		final N5FSReader n5 = new N5FSReader("src/test/resources/backward/" + base);
		assertTrue(n5.datasetExists(dsetPath));
		final DatasetAttributes attrs = n5.getDatasetAttributes(dsetPath);

		// equivalent to the assertTrue above, but be extra sure
		assertNotNull(attrs);

		byte value = 0;
		long[] p = new long[2];

		DataBlock<byte[]> b00 = n5.readBlock(dsetPath, attrs, p);
		assertNotNull(b00);
		assertArrayEquals(new int[]{5,4}, b00.getSize());
		assertArrayEquals(expectedData(20, value), b00.getData());

		p[0] = 1;
		p[1] = 0;
		value++;
		DataBlock<byte[]> b10 = n5.readBlock(dsetPath, attrs, p);
		assertNotNull(b10);
		assertArrayEquals(new int[]{2,4}, b10.getSize());
		assertArrayEquals(expectedData(8, value), b10.getData());

		p[0] = 0;
		p[1] = 1;
		value++;
		DataBlock<byte[]> b01 = n5.readBlock(dsetPath, attrs, p);
		assertNotNull(b01);
		assertArrayEquals(new int[]{5,1}, b01.getSize());
		assertArrayEquals(expectedData(5, value), b01.getData());

		p[0] = 1;
		p[1] = 1;
		value++;
		DataBlock<byte[]> b11 = n5.readBlock(dsetPath, attrs, p);
		assertNotNull(b11);
		assertArrayEquals(new int[]{2,1}, b11.getSize());
		assertArrayEquals(expectedData(2, value), b11.getData());

		n5.close();
	}

	@Test
	public void testBlockData() throws IOException {

		final N5FSReader n5Legacy = new N5FSReader("src/test/resources/backward/" + writeVersion);
		final URI uriLegacy = n5Legacy.getURI();

		final File basePath = Files.createTempDirectory("n5-blockDataTest-").toFile();
		basePath.delete();
		basePath.mkdir();
		basePath.deleteOnExit();

		N5FSWriter n5My = CreateSampleData.createSampleData(
				basePath.getCanonicalPath(), writeDataset, new RawCompression());
		URI uriMy = n5My.getURI();

		// check attributes
		final JsonElement attrsLegacy = ((GsonKeyValueN5Reader)n5Legacy).getAttributes(writeDataset);
		final JsonElement attrsMy = ((GsonKeyValueN5Reader)n5My).getAttributes(writeDataset);
		assertEquals(attrsLegacy, attrsMy);

		final KeyValueAccess kva = n5My.getKeyValueAccess();
		for (final String path : writePathsToTest) {
			final byte[] dataMy = read(kva, kva.compose(uriMy, writeDataset, path));
			final byte[] dataLegacy = read(kva, kva.compose(uriLegacy, writeDataset, path));
			assertArrayEquals(dataLegacy, dataMy);
		}

		n5My.remove();
		n5My.close();
		n5Legacy.close();
	}

	private byte[] read(KeyValueAccess kva, String path) {

		int N = (int)kva.size(path);
		byte[] data = new byte[N];
		try (LockedChannel ch = kva.lockForReading(path);
				InputStream is = ch.newInputStream();) {

			is.read(data);
		} catch (IOException e) {
			return null;
		}
		return data;
	}

	private static byte[] expectedData(int size, byte value ) {
		byte[] data = new byte[size];
		Arrays.fill(data, value);
		return data;
	}

}
