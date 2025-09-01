package org.janelia.saalfeldlab.n5.readdata;

import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

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
		 * @return location of the segment, or null if the segment is not contained in this ReadData
		 */
		SegmentLocation location(Segment segment);

		/**
		 * @return all segments contained in this {@code ReadData}.
		 */
		// TODO: return Collection<Segment> instead? Or Segment[]?
		//       Maybe: ordered by offset then length? Then offset of first
		//       segment and offset+length of last segment could be used to
		//       determine tight bounds of slice containing all segments.
		List<Segment> segments();

		// TODO: return wrapper of readData with one segment comprising the whole ReadData
		//       length of segment could return readData.length()
		static SegmentedReadData wrap(ReadData readData) {
			throw new UnsupportedOperationException("TODO");
		}

		// TODO: return wrapper of readData with (new) segments at the given locations
		static SegmentedReadData wrap(ReadData readData, SegmentLocation[] locations) {
			throw new UnsupportedOperationException("TODO");
		}

		// TODO: return a SegmentedReadData wrapping a slice containing exactly the given segment
		// throws IllegalArgumentException if segment is not contained in this ReadData
		SegmentedReadData slice(Segment segment) throws IllegalArgumentException, N5IOException;

		// TODO: has all segments fully contained in requested slice.
		@Override
		SegmentedReadData slice(final long offset, final long length) throws N5IOException;

	}

	// TODO: make Segment and SegmentLocation interfaces? Then we could make a
	//       class that implements both for use in SegmentedReadData implementation.

	/**
	 * A segment in a ReadData.
	 * <p>
	 * Note that segments use object identity.
	 * <p>
	 *
	 */
	public static class Segment {

		private final ReadData source;

		// TODO: add Segment id for debugging ...

		public Segment(final ReadData source) {

			this.source = source;
		}
	}

	public static class SegmentLocation {

		private final long offset;
		private final long length;

		public SegmentLocation(final long offset, final long length) {
			this.offset = offset;
			this.length = length;
		}

		public long offset() {
			return offset;
		}

		public long length() {
			return length;
		}
	}


	public static class ReadDataWrapper implements ReadData {
		private final ReadData delegate;
	}


}
