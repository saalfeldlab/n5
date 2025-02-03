package org.janelia.saalfeldlab.n5.readdata;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

// not thread-safe
abstract class AbstractInputStreamReadData extends AbstractReadData {

	private SplittableReadData bytes;

	@Override
	public SplittableReadData splittable() throws IOException {
		if (bytes == null) {
			final byte[] data;
			final int length = (int) length();
			if (length >= 0) {
				data = new byte[length];
				new DataInputStream(inputStream()).readFully(data);
			} else {
				data = IOUtils.toByteArray(inputStream());
			}
			bytes = new ByteArraySplittableReadData(data).order(this.order());
		}
		return bytes;
	}

	@Override
	public byte[] allBytes() throws IOException, IllegalStateException {
		return splittable().allBytes();
	}
}
