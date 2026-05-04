package org.janelia.saalfeldlab.n5.readdata;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

// not thread-safe
class InputStreamReadData implements ReadData {

	private final InputStream inputStream;
	private final int length;

	InputStreamReadData(final InputStream inputStream, final int length) {
		this.inputStream = inputStream;
		this.length = length;
	}

	@Override
	public long length() {
		return (bytes != null) ? bytes.length() : length;
	}

	@Override
	public long requireLength() throws N5IOException {
		long l = length();
		if ( l >= 0 ) {
			return l;
		} else {
			return materialize().length();
		}
	}

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {
		materialize();
		return bytes.slice(offset, length);
	}

	@Override
	public ReadData limit(final long length) {
		return new InputStreamReadData(inputStream, (int)length);
	}

	private boolean inputStreamCalled = false;

	@Override
	public InputStream inputStream() throws IllegalStateException {
		if (bytes != null) {
			return bytes.inputStream();
		} else if (!inputStreamCalled) {
			inputStreamCalled = true;
			return inputStream;
		} else {
			throw new IllegalStateException("InputStream() already called");
		}
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {
		materialize();
		return bytes.allBytes();
	}

	private ByteArrayReadData bytes;

	@Override
	public ReadData materialize() throws N5IOException {
		if (bytes == null) {
			final byte[] data;
			if (length >= 0) {
				data = new byte[length];
				try (InputStream is = inputStream()) {
					new DataInputStream(is).readFully(data);
				} catch (IOException e) {
					throw new N5IOException(e);
				}
			} else {
				try (InputStream is = inputStream()) {
					data = IOUtils.toByteArray(is);
				} catch (IOException e) {
					throw new N5IOException(e);
				}
			}
			bytes = new ByteArrayReadData(data);
		}
		return this;
	}
}
