package org.janelia.saalfeldlab.n5.readdata;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;

class KeyValueAccessReadData extends AbstractInputStreamReadData {

	private final KeyValueAccess keyValueAccess;
	private final String normalPath;

	KeyValueAccessReadData(final KeyValueAccess keyValueAccess, final String normalPath) {
		this.keyValueAccess = keyValueAccess;
		this.normalPath = normalPath;
	}

	/**
	 * Open a {@code InputStream} on this data.
	 * <p>
	 * This will open a {@code LockedChannel} on the underlying {@code
	 * KeyValueAccess}. Make sure to {@code close()} the returned {@code
	 * InputStream} to release the underlying {@code LockedChannel}.
	 *
	 * @return an InputStream on this data
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 */
	@Override
	public InputStream inputStream() throws IOException {
		final LockedChannel channel = keyValueAccess.lockForReading(normalPath);
		return new FilterInputStream(channel.newInputStream()) {

			@Override
			public void close() throws IOException {
				in.close();
				channel.close();
			}
		};
	}
}
