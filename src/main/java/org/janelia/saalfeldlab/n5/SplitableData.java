package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface SplitableData {

	// TODO Caleb:
	//  Do we need a getter for this? If it's intended to be relative
	//  after a split, shouldn't offset always be (locally) 0?
	//  maybe only need (offset) during #split
	long getOffset();

	long getSize();

	InputStream newInputStream() throws IOException;
	OutputStream newOutputStream() throws IOException;

	SplitableData split(long offset, long size);
}
