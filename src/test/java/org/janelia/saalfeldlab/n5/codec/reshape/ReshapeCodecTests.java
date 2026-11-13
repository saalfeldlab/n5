package org.janelia.saalfeldlab.n5.codec.reshape;

import static org.janelia.saalfeldlab.n5.codec.reshape.ReshapeEntry.dims;
import static org.janelia.saalfeldlab.n5.codec.reshape.ReshapeEntry.infer;
import static org.janelia.saalfeldlab.n5.codec.reshape.ReshapeEntry.size;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Random;
import java.util.function.UnaryOperator;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.NameConfigAdapter;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.SourceBlockSize;
import org.janelia.saalfeldlab.n5.codec.dataset.ScaleOffsetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.transpose.TransposeCodecInfo;
import org.janelia.saalfeldlab.n5.shard.DatasetAccess;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;
import org.janelia.saalfeldlab.n5.shard.TestPositionValueAccess;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ReshapeCodecTests {

	private static final Random random = new Random(4321);

	/*
	 * shape logic
	 */

	@Test
	public void specExampleInN5Order() {

		// zarr example: chunk [100, 50, 64, 3], shape [[0, 1], [2], 3]
		final ReshapeCodecInfo info = ReshapeCodecInfo.of(size(3), dims(1), dims(2, 3));
		assertArrayEquals(new int[]{3, 64, 5000}, info.encodedBlockSize(new int[]{3, 64, 50, 100}));
	}

	@Test
	public void inferredDimension() {

		assertArrayEquals(new int[]{60}, ReshapeCodecInfo.of(infer()).encodedBlockSize(new int[]{3, 4, 5}));
		assertArrayEquals(new int[]{3, 20}, ReshapeCodecInfo.of(dims(0), infer()).encodedBlockSize(new int[]{3, 4, 5}));
		assertArrayEquals(new int[]{6, 2, 5}, ReshapeCodecInfo.of(size(6), infer(), dims(2)).encodedBlockSize(new int[]{3, 4, 5}));
	}

	@Test
	public void fixedSizes() {

		assertArrayEquals(new int[]{2, 30}, ReshapeCodecInfo.of(size(2), size(30)).encodedBlockSize(new int[]{3, 4, 5}));
		assertArrayEquals(new int[]{1, 3, 4, 5, 1},
				ReshapeCodecInfo.of(size(1), dims(0), dims(1), dims(2), size(1)).encodedBlockSize(new int[]{3, 4, 5}));
	}

	@Test
	public void invalidConfigurationsAreRejected() {

		// not strictly increasing
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(dims(1), dims(0)));
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(dims(1, 0), size(10), dims(3, 4)));
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(dims(3, 4), size(10), dims(0, 1)));
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(dims(0, 0)));
		// -1 at most once
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(infer(), infer()));
		// sizes must be positive, input dimensions non-negative
		assertThrows(N5Exception.class, () -> size(0));
		assertThrows(N5Exception.class, () -> size(-2));
		assertThrows(N5Exception.class, () -> dims(-1));
		// no null entries
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(size(2), null));
		// empty shape
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of());
	}

	@Test
	public void invalidShapesAreRejected() {

		final int[] a = {2, 3, 4};
		// product mismatch
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(size(10), size(10)).encodedBlockSize(a));
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(size(5), infer()).encodedBlockSize(a));
		// input dimension out of range
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(dims(0, 5)).encodedBlockSize(a));
		// group does not correspond to the raveled index: prod(B[:1]) = 8 != prod(A[:1]) = 2
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(infer(), dims(1)).encodedBlockSize(a));
		// prod(B[2:]) = 1 != prod(A[2:]) = 4
		assertThrows(N5Exception.class, () -> ReshapeCodecInfo.of(size(4), dims(1), size(2)).encodedBlockSize(a));
	}

	/*
	 * codec
	 */

	@Test
	public void encodeSharesDataAndDecodeRestoresShape() {

		final long[] dimensions = {10, 7, 5};
		final int[] chunkSize = {4, 4, 5};
		final ReshapeCodec<int[]> codec = codec(ReshapeCodecInfo.of(dims(0, 1), dims(2)), dimensions, chunkSize);

		// the codec info resolves both shapes up front
		assertArrayEquals(chunkSize, codec.getDecodedShape());
		assertArrayEquals(new int[]{16, 5}, codec.getEncodedShape());

		// full chunk
		final DataBlock<int[]> full = randomIntBlock(chunkSize, new long[]{1, 0, 0});
		final DataBlock<int[]> encodedFull = codec.encode(full);
		assertArrayEquals(new int[]{16, 5}, encodedFull.getSize());
		assertSame(full.getData(), encodedFull.getData());
		assertArrayEquals(full.getGridPosition(), encodedFull.getGridPosition());

		final DataBlock<int[]> decodedFull = codec.decode(encodedFull);
		assertArrayEquals(chunkSize, decodedFull.getSize());
		assertArrayEquals(full.getGridPosition(), decodedFull.getGridPosition());
		assertSame(full.getData(), decodedFull.getData());

		// a cropped edge chunk does not have the fixed decoded shape
		final DataBlock<int[]> edge = randomIntBlock(new int[]{2, 3, 5}, new long[]{2, 1, 0});
		assertThrows(N5Exception.class, () -> codec.encode(edge));
	}

	@Test
	public void codecRequiresMatchingShapes() {

		assertThrows(N5Exception.class, () -> new ReshapeCodec<int[]>(new int[]{4, 4}, new int[]{15}));
	}

	@Test
	public void decodeRejectsUnexpectedElementCount() {

		final ReshapeCodec<int[]> codec = codec(ReshapeCodecInfo.of(infer()), new long[]{8, 8}, new int[]{4, 4});
		final DataBlock<int[]> wrong = new IntArrayDataBlock(new int[]{7}, new long[]{0, 0}, new int[7]);
		assertThrows(N5Exception.class, () -> codec.decode(wrong));
	}

	@Test
	public void createFailsForIncompatibleChunkSize() {

		assertThrows(N5Exception.class, () -> codec(ReshapeCodecInfo.of(size(5), infer()), new long[]{8, 8}, new int[]{4, 4}));
	}

	/*
	 * serialization
	 */

	@Test
	public void serializationRoundTrips() {

		final Gson gson = gson();

		final ReshapeCodecInfo original = ReshapeCodecInfo.of(size(3), dims(1), dims(2, 3), infer());
		final JsonElement json = gson.toJsonTree(original, CodecInfo.class);
		assertEquals(
				JsonParser.parseString("{\"name\":\"n5-reshape\",\"configuration\":{\"shape\":[3,[1],[2,3],-1]}}"),
				json);

		final CodecInfo restored = gson.fromJson(json, CodecInfo.class);
		assertEquals(original, restored);
		assertEquals(original.hashCode(), restored.hashCode());
		assertEquals(json, gson.toJsonTree(restored, CodecInfo.class));
		assertArrayEquals(new int[]{3, 64, 5000, 1},
				((ReshapeCodecInfo)restored).encodedBlockSize(new int[]{3, 64, 50, 100}));
	}

	@Test
	public void invalidEntriesAreRejectedWhenDeserializing() {

		final Gson gson = gson();
		for (final String shape : new String[]{"[0]", "[-2]", "[1.5]", "[\"a\"]", "[[0, -1]]", "[[0.5]]", "[{}]"}) {
			final String json = "{\"name\":\"n5-reshape\",\"configuration\":{\"shape\":" + shape + "}}";
			assertThrows(shape, N5Exception.class, () -> gson.fromJson(json, CodecInfo.class));
		}
	}

	@Test
	public void invalidShapesAreRejectedAfterDeserializing() {

		// constraints across entries are checked when the codec info is used, since
		// deserialization sets the shape by reflection
		final Gson gson = gson();
		for (final String shape : new String[]{"[]", "[-1, -1]", "[[1], [0]]", "[[0, 0]]"}) {
			final String json = "{\"name\":\"n5-reshape\",\"configuration\":{\"shape\":" + shape + "}}";
			final ReshapeCodecInfo info = (ReshapeCodecInfo)gson.fromJson(json, CodecInfo.class);
			assertThrows(shape, N5Exception.class, () -> info.encodedBlockSize(new int[]{2, 3}));
		}
	}

	@Test
	public void reverseDimensionsConvertsBetweenZarrAndN5() {

		// the spec example in zarr order: chunk [100, 50, 64, 3], shape [[0, 1], [2], 3] -> [5000, 64, 3]
		final ReshapeEntry[] zarr = {dims(0, 1), dims(2), size(3)};
		final ReshapeEntry[] n5 = ReshapeEntry.reverseDimensions(zarr, 4);
		assertEquals(json("[3, [1], [2, 3]]"), json(n5));
		assertArrayEquals(new ReshapeEntry[]{size(3), dims(1), dims(2, 3)}, n5);
		assertEquals(json(zarr), json(ReshapeEntry.reverseDimensions(n5, 4)));
		assertArrayEquals(new int[]{3, 64, 5000}, ReshapeCodecInfo.of(n5).encodedBlockSize(new int[]{3, 64, 50, 100}));

		assertEquals(json("[-1, [0]]"), json(ReshapeEntry.reverseDimensions(new ReshapeEntry[]{dims(2), infer()}, 3)));
		assertThrows(N5Exception.class, () -> ReshapeEntry.reverseDimensions(new ReshapeEntry[]{dims(3)}, 3));
	}

	/*
	 * end to end, through DatasetAttributes
	 */

	@Test
	public void roundTripN5BlockCodec() {

		for (final ReshapeCodecInfo reshape : new ReshapeCodecInfo[]{
				ReshapeCodecInfo.of(dims(0, 1), dims(2)),
				ReshapeCodecInfo.of(infer()),
				ReshapeCodecInfo.of(dims(0), infer())}) {

			final long[] dimensions = {10, 7, 5};
			final int[] chunkSize = {4, 4, 5};
			final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.INT32,
					new N5BlockCodecInfo(), reshape);
			// the codec reshapes to a fixed shape, so edge chunks are written uncropped
			roundTripAllChunks(attributes, dimensions, chunkSize, false);
		}
	}

	@Test
	public void croppedEdgeChunkIsRejected() {

		final long[] dimensions = {10, 7, 5};
		final int[] chunkSize = {4, 4, 5};
		final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.INT32,
				new N5BlockCodecInfo(), ReshapeCodecInfo.of(dims(0, 1), dims(2)));
		final DatasetAccess<int[]> access = attributes.getDatasetAccess();

		final DataBlock<int[]> edge = randomIntBlock(new int[]{2, 3, 5}, new long[]{2, 1, 0});
		assertThrows(N5Exception.class, () -> access.writeChunk(new TestPositionValueAccess(), edge));
	}

	@Test
	public void roundTripRawBlockCodec() {

		final long[] dimensions = {10, 7, 5};
		final int[] chunkSize = {4, 4, 5};
		final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.INT32,
				new RawBlockCodecInfo(), ReshapeCodecInfo.of(dims(0, 1), dims(2)));

		// the raw block codec stores full chunks only
		roundTripAllChunks(attributes, dimensions, chunkSize, false);
	}

	@Test
	public void roundTripReshapeThenScaleOffset() {

		final long[] dimensions = {6, 6};
		final int[] chunkSize = {3, 6};
		final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.FLOAT64,
				new N5BlockCodecInfo(), ReshapeCodecInfo.of(infer()), new ScaleOffsetCodecInfo(2.0, 1.0));

		final PositionValueAccess store = new TestPositionValueAccess();
		final DatasetAccess<double[]> access = attributes.getDatasetAccess();

		final double[] data = new double[18];
		for (int i = 0; i < data.length; ++i)
			data[i] = 0.5 * i;
		final long[] gridPosition = {1, 0};
		access.writeChunk(store, new DoubleArrayDataBlock(chunkSize, gridPosition, data.clone()));

		final DataBlock<double[]> read = access.readChunk(store, gridPosition);
		assertArrayEquals(chunkSize, read.getSize());
		assertArrayEquals(data, read.getData(), 1e-12);
	}

	@Test
	public void roundTripReshapeThenTranspose() {

		// transpose sees the reshaped (2D) block
		final long[] dimensions = {4, 3, 5};
		final int[] chunkSize = {4, 3, 5};
		final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.INT32,
				new N5BlockCodecInfo(), ReshapeCodecInfo.of(dims(0, 1), dims(2)),
				new TransposeCodecInfo(new int[]{1, 0}));

		roundTripAllChunks(attributes, dimensions, chunkSize, true);
	}

	@Ignore("TransposeCodec.encode returns blocks with the decoded (unpermuted) size, so a following reshape "
			+ "sees the wrong shape. Enable once the transpose encoded-size fix lands.")
	@Test
	public void roundTripTransposeThenReshape() {

		final long[] dimensions = {4, 3, 5};
		final int[] chunkSize = {4, 3, 5};
		// transpose [4, 3, 5] -> [3, 5, 4], then merge the first two dimensions -> [15, 4]
		final ReshapeAttributes attributes = new ReshapeAttributes(dimensions, chunkSize, DataType.INT32,
				new N5BlockCodecInfo(), new TransposeCodecInfo(new int[]{1, 2, 0}),
				ReshapeCodecInfo.of(dims(0, 1), dims(2)));

		roundTripAllChunks(attributes, dimensions, chunkSize, true);
	}

	/*
	 * helpers
	 */

	private static void roundTripAllChunks(final ReshapeAttributes attributes, final long[] dimensions,
			final int[] chunkSize, final boolean cropEdges) {

		final PositionValueAccess store = new TestPositionValueAccess();
		final DatasetAccess<int[]> access = attributes.getDatasetAccess();

		final int n = dimensions.length;
		final long[] gridSize = new long[n];
		long numChunks = 1;
		for (int d = 0; d < n; ++d) {
			gridSize[d] = (dimensions[d] + chunkSize[d] - 1) / chunkSize[d];
			numChunks *= gridSize[d];
		}

		final SourceBlockSize cropped = SourceBlockSize.of(dimensions, chunkSize, UnaryOperator.identity());
		final DataBlock<?>[] written = new DataBlock[(int)numChunks];
		for (int i = 0; i < numChunks; ++i) {
			final long[] gridPosition = gridPosition(i, gridSize);
			final int[] size = cropEdges ? cropped.at(gridPosition) : chunkSize.clone();
			final DataBlock<int[]> block = randomIntBlock(size, gridPosition);
			written[i] = new IntArrayDataBlock(size.clone(), gridPosition.clone(), block.getData().clone());
			access.writeChunk(store, block);
		}

		for (int i = 0; i < numChunks; ++i) {
			final long[] gridPosition = gridPosition(i, gridSize);
			final DataBlock<int[]> read = access.readChunk(store, gridPosition);
			assertArrayEquals("size at " + i, written[i].getSize(), read.getSize());
			assertArrayEquals("grid position at " + i, gridPosition, read.getGridPosition());
			assertArrayEquals("data at " + i, (int[])written[i].getData(), read.getData());
		}
	}

	private static long[] gridPosition(long index, final long[] gridSize) {

		final long[] position = new long[gridSize.length];
		for (int d = 0; d < gridSize.length; ++d) {
			position[d] = index % gridSize[d];
			index /= gridSize[d];
		}
		return position;
	}

	private static JsonElement json(final ReshapeEntry[] shape) {

		return new Gson().toJsonTree(shape);
	}

	private static JsonElement json(final String shape) {

		return JsonParser.parseString(shape);
	}

	private static Gson gson() {

		final GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(DataType.class, new DataType.JsonAdapter());
		gsonBuilder.registerTypeHierarchyAdapter(CodecInfo.class, NameConfigAdapter.getJsonAdapter(CodecInfo.class));
		return gsonBuilder.create();
	}

	@SuppressWarnings("unchecked")
	private static ReshapeCodec<int[]> codec(final ReshapeCodecInfo info, final long[] dimensions, final int[] chunkSize) {

		return (ReshapeCodec<int[]>)info.create(null, DataType.INT32,
				SourceBlockSize.of(dimensions, chunkSize, UnaryOperator.identity()));
	}

	private static DataBlock<int[]> randomIntBlock(final int[] size, final long[] gridPosition) {

		final int[] data = new int[DataBlock.getNumElements(size)];
		for (int i = 0; i < data.length; ++i)
			data[i] = random.nextInt();
		return new IntArrayDataBlock(size, gridPosition, data);
	}

	private static class ReshapeAttributes extends DatasetAttributes {

		ReshapeAttributes(final long[] dimensions, final int[] chunkSize, final DataType dataType,
				final BlockCodecInfo blockCodecInfo, final DatasetCodecInfo... datasetCodecInfos) {

			super(dimensions, chunkSize, dataType, blockCodecInfo, datasetCodecInfos, new DataCodecInfo[0]);
		}

		@Override // to make this accessible for the test
		protected <T> DatasetAccess<T> getDatasetAccess() {

			return super.getDatasetAccess();
		}
	}
}
