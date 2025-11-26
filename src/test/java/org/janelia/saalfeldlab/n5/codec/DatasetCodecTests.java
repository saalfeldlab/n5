package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.janelia.saalfeldlab.n5.codec.transpose.TransposeCodecInfo;
import org.junit.Test;

public class DatasetCodecTests {

	@Test
	public void testTransposeCodecSimplification() throws Exception {

		// 2d
		final TransposeCodecInfo id2 = new TransposeCodecInfo(new int[]{0, 1});
		final TransposeCodecInfo rev2 = new TransposeCodecInfo(new int[]{1, 0});

		assertNull(TransposeCodecInfo.concatenate(null));
		assertEquals(id2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{id2}));
		assertEquals(rev2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev2}));

		assertEquals(rev2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev2, id2}));
		assertEquals(rev2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{id2, rev2, id2}));

		assertEquals(id2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev2, rev2}));
		assertEquals(id2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev2, rev2, rev2, rev2}));
		assertEquals(id2, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{id2, rev2, id2, rev2, rev2, rev2}));

		// 3d
		final TransposeCodecInfo id3 = new TransposeCodecInfo(new int[]{0, 1, 2});
		final TransposeCodecInfo rev3 = new TransposeCodecInfo(new int[]{2, 1, 0});

		final TransposeCodecInfo t021 = new TransposeCodecInfo(new int[]{0, 2, 1});
		final TransposeCodecInfo t102 = new TransposeCodecInfo(new int[]{1, 0, 2});
		final TransposeCodecInfo t120 = new TransposeCodecInfo(new int[]{1, 2, 0});
		final TransposeCodecInfo t201 = new TransposeCodecInfo(new int[]{2, 0, 1});

		assertEquals(id3, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{id3}));
		assertEquals(rev3, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev3}));

		assertEquals(rev3, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev3, id3}));
		assertEquals(rev3, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{id3, rev3, id3}));

		assertEquals(t102, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{rev3, t102, t021}));
		assertEquals(t201, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{t021, t102}));
		assertEquals(t120, TransposeCodecInfo.concatenate(new TransposeCodecInfo[]{t102, t021}));
	}
}
