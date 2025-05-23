package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;

public class FileSplittableReadData implements SplittableReadData {

	private ByteArraySplittableReadData materialized;

	private final KeyValueAccess kva;
	private final String normalKey;
	private final long offset;
	private long length;

	public FileSplittableReadData(KeyValueAccess kva, String normalKey, long offset, long length) {
		this.kva = kva;
		this.normalKey = normalKey;
		this.offset = offset;
		this.length = length;
	}

	public FileSplittableReadData(KeyValueAccess kva, String normalKey, long offset) {
		this(kva, normalKey, offset, -1);
	}

	@Override
	public long length() {
		if (materialized != null)
			return materialized.length();

		if (length < 0) {
			try {
				length = kva.size(normalKey);
			} catch (IOException e) {}
		}
		return length;
	}

	@Override
	public InputStream inputStream() throws IOException, IllegalStateException {
		return materialize().inputStream();
	}

	@Override
	public byte[] allBytes() throws IOException, IllegalStateException {
		return materialize().allBytes();
	}

	@Override
	public SplittableReadData materialize() throws IOException {
		if (materialized == null)
			read();

		return materialized;
	}

	private void read() throws IOException {

		final FileChannel channel;
		try {
			channel = FileChannel.open(Paths.get(normalKey), StandardOpenOption.READ);
		} catch (final NoSuchFileException e) {
			throw new N5Exception.N5NoSuchKeyException(e);
		}
		channel.position(offset);

		if (length > Integer.MAX_VALUE)
			throw new IOException("Attempt to materialize too large data");

		final int sz = (int)(length < 0 ? channel.size() : (int)length);
		final byte[] data = new byte[sz];
		final ByteBuffer buf = ByteBuffer.wrap(data);
		channel.read(buf);
		materialized = new ByteArraySplittableReadData(data);
	}

	@Override
	public ReadData slice(long offset, long length) throws IOException {

		if (materialized != null)
			return materialize().slice(offset, length);

		return new FileSplittableReadData(kva, normalKey, this.offset + offset, length);
	}

	@Override
	public Pair<ReadData, ReadData> split(long pivot) throws IOException {

		if (materialized != null)
			return materialize().split(pivot);

		final long offsetL = 0;
		final long lenL = pivot;

		final long offsetR = offset + pivot;
		final long lenR = this.length - pivot;

		return new ImmutablePair<ReadData, ReadData>(
				 new FileSplittableReadData(kva, normalKey, offsetL, lenL),
				 new FileSplittableReadData(kva, normalKey, offsetR, lenR));
	}

}
