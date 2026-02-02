package org.janelia.saalfeldlab.n5.readdata.kva;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Test;

public class SliceTrackingLazyReadTests {

	@Test
	public void testDefaultSliceTracking() throws N5IOException {

		/**
		 * 1. Create sample ReadData from byte[]
		 * 2. Create a DummyLazy Read
		 * 3. Make a DefaultSliceTrackingLazyRead
		 * 4. Create a list of ranges
		 * 5. Call prefetch
		 * 6. Ensure the correct number of materialize calls were made (always 1 for DefaultSliceTrackingLazyRead)
		 * 7. Verify the stored slices contain the correct range
		 */

		// 1-2. Create a DummyLazyRead with 64 bytes
		DummyLazyRead dummyLazyRead = createDummyLazyRead(64);

		// 3. Make a testable DefaultSliceTrackingLazyRead
		TestableDefaultSliceTracker sliceTracking = new TestableDefaultSliceTracker(dummyLazyRead);

		// 4. Create a list of ranges (two non-overlapping ranges with a gap)
		List<Range> ranges = Arrays.asList(
			Range.at(10, 5),   // offset 10, length 5 (bytes 10-14)
			Range.at(50, 10)   // offset 50, length 10 (bytes 50-59)
		);

		// 5. Call prefetch
		sliceTracking.prefetch(ranges);

		// 6. Ensure exactly 1 materialize call was made
		// DefaultSliceTrackingLazyRead creates a single large slice covering all ranges
		assertEquals("DefaultSliceTrackingLazyRead should make exactly 1 materialize call",
			1, dummyLazyRead.getNumMaterializeCalls());

		// 7. Verify the stored slice covers the entire range from offset 10 with length 50
		assertStoredSlices(sliceTracking, Arrays.asList(
			Range.at(10, 50)   // Single slice covering offset 10-59
		));

	}

	@Test
	public void testAggregatingSliceTracking() throws N5IOException {

		/**
		 * 1. Create sample ReadData from byte[]
		 * 2. Create a DummyLazyRead
		 * 3. Make an AggregatingSliceTrackingLazyRead
		 * 4. Create a list of ranges
		 * 5. Call prefetch
		 * 6. Ensure the correct number of materialize calls were made (one per aggregated range)
		 * 7. Verify the stored slices contain the correct ranges
		 */

		// 1-2. Create a DummyLazyRead with 64 bytes
		DummyLazyRead dummyLazyRead = createDummyLazyRead(64);

		// 3. Make a testable AggregatingSliceTrackingLazyRead
		TestableAggregatingSliceTracker sliceTracking = new TestableAggregatingSliceTracker(dummyLazyRead);

		/*
		 * Non-adjacent ranges
		 */
		// 4. Create a list of ranges (two non-overlapping ranges with a gap)
		List<Range> ranges = Arrays.asList(
			Range.at(10, 5),   // offset 10, length 5 (bytes 10-14)
			Range.at(50, 10)   // offset 50, length 10 (bytes 50-59)
		);

		// 5. Call prefetch
		sliceTracking.prefetch(ranges);

		// 6. Ensure exactly 2 materialize calls were made
		// AggregatingSliceTrackingLazyRead aggregates overlapping/adjacent ranges
		// Since these ranges are not adjacent or overlapping, it makes 2 separate calls
		assertEquals("AggregatingSliceTrackingLazyRead should make 2 materialize calls for non-adjacent ranges",
			2, dummyLazyRead.getNumMaterializeCalls());

		// 7. Verify the stored slices contain two separate ranges
		assertStoredSlices(sliceTracking, Arrays.asList(
			Range.at(10, 5),   // First slice
			Range.at(50, 10)   // Second slice
		));

		/*
		 * Adjacent ranges
		 */

		// new sliceTracking instance to clear slices
		sliceTracking = new TestableAggregatingSliceTracker(dummyLazyRead);
		dummyLazyRead.resetNumMaterializeCalls();

		// 4. Create a list of three contiguous ranges
		List<Range> adjacentRanges = Arrays.asList(
			Range.at(10, 5),   // offset 10, length 5 (bytes 10-14)
			Range.at(15, 10),  // offset 15, length 10 (bytes 15-24)
			Range.at(25, 5)    // offset 25, length 5 (bytes 25-29)
		);

		// 5. Call prefetch
		sliceTracking.prefetch(adjacentRanges);

		// 6. Ensure exactly 1 materialize call was made
		// AggregatingSliceTrackingLazyRead should aggregate these three contiguous ranges
		// into a single range from offset 10 to 30, with length 20
		assertEquals("AggregatingSliceTrackingLazyRead should make 1 materialize call for contiguous ranges",
			1, dummyLazyRead.getNumMaterializeCalls());

		// 7. Verify the stored slices now contain three ranges total:
		// the two from the first prefetch plus one aggregated range from the second prefetch
		assertStoredSlices(sliceTracking, Arrays.asList(
			Range.at(10, 20)  // Aggregated range 
		));

	}

	private static DummyLazyRead createDummyLazyRead(int size) {
		byte[] data = new byte[size];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte)i;
		}
		return new DummyLazyRead(ReadData.from(data));
	}

	/**
	 * Helper method to verify that stored slices match expected ranges.
	 *
	 * @param sliceTracking the SliceTrackingLazyRead instance
	 * @param expectedRanges the expected ranges stored in slices
	 */
	private static void assertStoredSlices(TestableSliceTracker sliceTracking, List<Range> expectedRanges) {
		// Access protected slices field via a test helper
		List<Range> actualSlices = sliceTracking.getSlices();

		assertEquals("Number of stored slices should match", expectedRanges.size(), actualSlices.size());

		for (int i = 0; i < expectedRanges.size(); i++) {
			Range expected = expectedRanges.get(i);
			Range actual = actualSlices.get(i);
			assertEquals("Slice " + i + " offset should match", expected.offset(), actual.offset());
			assertEquals("Slice " + i + " length should match", expected.length(), actual.length());
		}
	}

	/**
	 * Testable wrapper for DefaultSliceTrackingLazyRead that exposes slices.
	 */
	static class TestableDefaultSliceTracker extends DefaultSliceTrackingLazyRead implements TestableSliceTracker {
		public TestableDefaultSliceTracker(LazyRead delegate) {
			super(delegate);
		}

		@Override
		public List<Range> getSlices() {
			return java.util.Collections.unmodifiableList(slices);
		}
	}

	/**
	 * Testable wrapper for AggregatingSliceTrackingLazyRead that exposes slices.
	 */
	static class TestableAggregatingSliceTracker extends AggregatingSliceTrackingLazyRead implements TestableSliceTracker {
		public TestableAggregatingSliceTracker(LazyRead delegate) {
			super(delegate);
		}

		@Override
		public List<Range> getSlices() {
			return java.util.Collections.unmodifiableList(slices);
		}
	}

	/**
	 * Interface for testable slice trackers.
	 */
	interface TestableSliceTracker {
		List<Range> getSlices();
	}

	static class DummyLazyRead implements LazyRead {
		
		private ReadData data;
		private int numMaterializeCalls = 0;

		public DummyLazyRead( ReadData data ) {
			this.data = data;
		}

		@Override
		public void close() throws IOException {
			// no op
		}

		@Override
		public ReadData materialize(long offset, long length) throws N5IOException {

			numMaterializeCalls++;
			return data.slice(offset, length).materialize();
		}

		@Override
		public long size() throws N5IOException {

			return data.length();
		}

		public int getNumMaterializeCalls() {

			return numMaterializeCalls;
		}

		public void resetNumMaterializeCalls() {

			numMaterializeCalls = 0;
		}
		
	}
}
