package org.janelia.saalfeldlab.n5.readdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

public class RangeTests {

	@Test
	public void testAggregate() {

		List<Range> nonOverlapping = Stream.of(Range.at(0, 5), Range.at(12, 2)).collect(Collectors.toList());
		assertSame(nonOverlapping, Range.aggregate(nonOverlapping));

		List<Range> nonOverlappingRev = Stream.of(Range.at(12, 2), Range.at(0, 5)).collect(Collectors.toList());
		assertSame(nonOverlappingRev, Range.aggregate(nonOverlappingRev));

		/**
		 * 0 1 2 3 4 5 
		 * x x x x x - 
		 * - x - - - -
		 */
		List<Range> containing = Stream.of(Range.at(0, 5), Range.at(1, 1)).collect(Collectors.toList());
		assertEquals(Collections.singletonList(Range.at(0, 5)), Range.aggregate(containing));

		/**
		 * 0 1 2 3 4 5 6
		 * x x x x x - -
		 * - - x x x x x
		 */
		List<Range> overlapping = Stream.of(Range.at(0, 5), Range.at(2, 5)).collect(Collectors.toList());
		assertEquals(Collections.singletonList(Range.at(0, 7)), Range.aggregate(overlapping));

		/**
		 * 0 1 2 3 4 5
		 * x x x x x -
		 * - - - - - x
		 */
		List<Range> adjacent = Stream.of(Range.at(0, 5), Range.at(5, 1)).collect(Collectors.toList());
		assertEquals(Collections.singletonList(Range.at(0, 6)), Range.aggregate(adjacent));

		/**
		 * 0 1 2 3 4 5
		 * - - - x x -
		 * x x - - - -
		 * - - - - - x
		 */
		List<Range> three = Stream.of(Range.at(3, 2), Range.at(0, 2), Range.at(5, 1)).collect(Collectors.toList());
		assertEquals(Stream.of(Range.at(0, 2), Range.at(3, 3)).collect(Collectors.toList()), Range.aggregate(three));
	}

}
