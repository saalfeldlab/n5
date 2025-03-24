package org.janelia.saalfeldlab.n5.readdata;

import java.io.InputStream;

// not thread-safe
class InputStreamReadData extends AbstractInputStreamReadData {

	private final InputStream inputStream;
	private final int length;

	InputStreamReadData(final InputStream inputStream, final int length) {
		this.inputStream = inputStream;
		this.length = length;
	}

	@Override
	public long length() {
		return length;
	}

	private boolean inputStreamCalled = false;

	@Override
	public InputStream inputStream() throws IllegalStateException {
		if (inputStreamCalled) {
			throw new IllegalStateException("InputStream() already called");
		} else {
			inputStreamCalled = true;
			return inputStream;
		}
	}
}
