package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;

public class N5CachedFSTest extends N5FSTest {


	@Override protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(createTempN5DirectoryPath(), true);
	}

	@Override protected N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException {

		if (!new File(location).exists()) {
			tmpFiles.add(location);
		}
		return new N5FSWriter(location, gson, true);
	}

	@Override protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

		return new N5FSReader(location, gson, true);
	}
}
