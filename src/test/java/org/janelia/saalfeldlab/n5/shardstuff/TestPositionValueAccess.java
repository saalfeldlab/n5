package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

public class TestPositionValueAccess implements PositionValueAccess {

	private final Map<Key, byte[]> map = new HashMap<>();

	@Override
	public ReadData get(final long[] key) {
		final byte[] bytes = map.get(new Key(key));
		return bytes == null ? null : ReadData.from(bytes);
	}

	@Override
	public void put(final long[] key, final ReadData data) {
		final byte[] bytes = data == null ? null : data.allBytes();
		map.put(new Key(key), bytes);
	}

	@Override
	public void remove(final long[] key) throws N5IOException {
		map.remove(new Key(key));
	}

	private static class Key {

		private final long[] data;

		Key(long[] data) {

			this.data = data;
		}

		@Override
		public final boolean equals(final Object o) {

			if (!(o instanceof Key)) {
				return false;
			}
			final Key key = (Key)o;
			return Arrays.equals(data, key.data);
		}

		@Override
		public int hashCode() {

			return Arrays.hashCode(data);
		}
	}
}
