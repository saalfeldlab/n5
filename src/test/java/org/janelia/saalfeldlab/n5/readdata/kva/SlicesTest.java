package org.janelia.saalfeldlab.n5.readdata.kva;

import java.util.ArrayList;
import java.util.List;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SlicesTest {

	private List<Range> createSlices(final long[] offsets, final long[] lengths) {
		final List<Range> slices = new ArrayList<>();
		for (int i = 0; i < offsets.length; ++i) {
			slices.add(Range.at(offsets[i], lengths[i]));
		}
		return slices;
	}

	@Test
	public void testFindContaining() {

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,6)    [-----------]
		// (6,4)            [---------]
		// (8,6)                [-----------]

		final List<Range> slices = createSlices(
				new long[] {2, 6, 8},
				new long[] {6, 4, 6});
		Range slice;

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (1,1)  [-]
		slice = Slices.findContainingSlice(slices, 1, 1);
		assertEquals(null, slice);

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,1)    [-]
		slice = Slices.findContainingSlice(slices, 2, 1);
		assertEquals(2, slice.offset());

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,6)    [-----------]
		slice = Slices.findContainingSlice(slices, 2, 6);
		assertEquals(2, slice.offset());

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,7)    [-------------]
		slice = Slices.findContainingSlice(slices, 2, 7);
		assertEquals(null, slice);

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (6,4)            [-------]
		slice = Slices.findContainingSlice(slices, 6, 4);
		assertEquals(6, slice.offset());

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (8,2)                [---]
		slice = Slices.findContainingSlice(slices, 8, 2);
		assertEquals(8, slice.offset());

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (12,2)                       [---]
		slice = Slices.findContainingSlice(slices, 12, 2);
		assertEquals(8, slice.offset());

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (12,3)                       [-----]
		slice = Slices.findContainingSlice(slices, 12, 3);
		assertEquals(null, slice);

		//       0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (14,1)                           [-]
		slice = Slices.findContainingSlice(slices, 14, 1);
		assertEquals(null, slice);
	}


	@Test
	public void testAddSlice() {

		//        0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,6)     [-----------]
		// (6,4)             [---------]
		// (8,6)                 [-----------]
		final List<Range> initial = createSlices(
				new long[] {2, 6, 8},
				new long[] {6, 4, 6});
		List<Range> slices;


		slices = new ArrayList<>(initial);
		Slices.addSlice(slices, Range.at(0, 1));
		//        0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (0,1) [-]
		// (2,6)     [-----------]
		// (6,4)             [---------]
		// (8,6)                 [-----------]
		assertEquals(createSlices(
				new long[] {0, 2, 6, 8},
				new long[] {1, 6, 4, 6}), slices);


		slices = new ArrayList<>(initial);
		Slices.addSlice(slices, Range.at(0, 16));
		//        0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (0,16)[-------------------------------]
		assertEquals(createSlices(
				new long[] {0},
				new long[] {16}), slices);


		slices = new ArrayList<>(initial);
		Slices.addSlice(slices, Range.at(2, 8));
		//        0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (2,8)     [-----------------]
		// (8,6)                 [-----------]
		assertEquals(createSlices(
				new long[] {2, 8},
				new long[] {8, 6}), slices);


		slices = new ArrayList<>(initial);
		Slices.addSlice(slices, Range.at(1, 10));
		//        0 1 2 3 4 5 6 7 8 9 A B C D E F
		// (1,10)  [---------------------]
		// (8,6)                 [-----------]
		assertEquals(createSlices(
				new long[] {1, 8},
				new long[] {10, 6}), slices);
	}
}
