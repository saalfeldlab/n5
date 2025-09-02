package org.janelia.saalfeldlab.n5.readdata.segment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public class SegmentStuff {

	public interface SegmentedReadData extends ReadData {

		/**
		 * Returns the {@code SegmentLocation} of {@code segment} in this {@code ReadData}.
		 * <p>
		 * Note that this {@code ReadData} is not necessarily the source of the segment.
		 * <p>
		 * The returned {@code SegmentLocation} may be {@code {offset=0, index=-1}}, which
		 * means that the segment comprises this whole {@code ReadData} (and the length of
		 * this {@code ReadData} is not yet known.
		 *
		 * @param segment
		 * 		the segment id
		 *
		 * @return location of the segment, or null
		 * @throws IllegalArgumentException if the segment is not contained in this ReadData
		 */
		SegmentLocation location(Segment segment) throws IllegalArgumentException;

		/**
		 * @return all segments contained in this {@code ReadData}.
		 */
		// Order is the same as the SegmentLocations given at construction
		List<? extends Segment> segments();

		// TODO: return wrapper of readData with one segment comprising the whole ReadData
		//       length of segment could return readData.length()
		static SegmentedReadData wrap(ReadData readData) {
			return SegmentedReadDataImpl.wrap(readData);
		}

		/**
		 * Wrap {@code readData} and create segments at the given locations.
		 */
		// TODO: does not assume ordered locations. to not lose track, this method
		//       should return Pair<SegmentedReadData, Segment[]> where segments are in
		//       the same order as locations.
		// TODO: use List<SegmentLocation> instead of SegmentLocation[]
		static SegmentedReadData wrap(ReadData readData, SegmentLocation... locations) {
			return SegmentedReadDataImpl.wrap(readData, locations);
		}

		@Override
		default SegmentedReadData limit(final long length) throws N5IOException {
			return slice(0, length);
		}

		/**
		 * Return a {@code SegmentedReadData} wrapping a slice containing exactly the given segment.
		 *
		 * @param segment
		 * @return
		 * @throws IllegalArgumentException if the segment is not contained in this ReadData
		 * @throws N5IOException
		 */
		SegmentedReadData slice(Segment segment) throws IllegalArgumentException, N5IOException;

		// TODO: has all segments fully contained in requested slice.
		@Override
		SegmentedReadData slice(final long offset, final long length) throws N5IOException;

		@Override
		SegmentedReadData materialize() throws N5IOException;
	}

	/**
	 * A particular segment in a source {@code ReadData}.
	 */
	public interface Segment {

		ReadData source();
	}

	public interface SegmentLocation {

		Comparator<SegmentLocation> COMPARATOR = Comparator
				.comparingLong(SegmentLocation::offset)
				.thenComparingLong(SegmentLocation::length);

		long offset();

		long length();

		static SegmentLocation at(final long offset, final long length) {
			return new SegmentLocationImpl(offset, length);
		}
	}

	static class SegmentLocationImpl implements SegmentLocation {

		private final long offset;
		private final long length;

		public SegmentLocationImpl(final long offset, final long length) {
			this.offset = offset;
			this.length = length;
		}

		@Override
		public long offset() {
			return offset;
		}

		@Override
		public long length() {
			return length;
		}

		@Override
		public String toString() {
			return "SegmentLocation{" +
					"offset=" + offset +
					", length=" + length +
					'}';
		}
	}
}
