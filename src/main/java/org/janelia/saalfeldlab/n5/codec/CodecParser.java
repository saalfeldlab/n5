package org.janelia.saalfeldlab.n5.codec;

import java.util.ArrayList;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;

public class CodecParser {

	public DatasetCodecInfo[] datasetCodecInfos;
	public BlockCodecInfo blockCodecInfo;
	public DataCodecInfo[] dataCodecInfos;

	public CodecParser(CodecInfo[] codecs) {

		parse(codecs);
	}

	private void parse(CodecInfo[] codecs) {

		final ArrayList<DataCodecInfo> dataCodecList = new ArrayList<>();
		final ArrayList<DatasetCodecInfo> datasetCodecList = new ArrayList<>();

		boolean foundBlockCodec = false;

		int i = 0;
		int blockCodecIndex = -1;
		for (CodecInfo codec : codecs) {
			if (!foundBlockCodec) {

				if (codec instanceof BlockCodecInfo) {
					blockCodecInfo = (BlockCodecInfo)codec;
					foundBlockCodec = true;
					blockCodecIndex = i;
				} else if (codec instanceof DatasetCodecInfo)
					datasetCodecList.add((DatasetCodecInfo)codec);
				else
					throw new N5Exception("Codec at index " + i + " is a DataCodec, but came before a BlockCodec.");

			} else if (codec instanceof BlockCodecInfo)
				throw new N5Exception("Codec at index " + i + " is a BlockCodec, but came after a BlockCodec at position " + blockCodecIndex);
			else if (codec instanceof DatasetCodecInfo)
				throw new N5Exception("Codec at index " + i + " is a DatasetCodec, but came after a BlockCodec at position " + blockCodecIndex);
			else
				dataCodecList.add((DataCodecInfo)codec);

			i++;
		}

		datasetCodecInfos = datasetCodecList.stream().toArray(n -> new DatasetCodecInfo[n]);
		dataCodecInfos = dataCodecList.stream().toArray(n -> new DataCodecInfo[n]);
	}

	/**
	 * Flattens the codecs of the given attributes into a single array, in the order
	 * {@link #parse} expects: dataset ({@code array -> array}) codecs, then the block
	 * ({@code array -> bytes}) codec, then data ({@code bytes -> bytes}) codecs.
	 *
	 * @param attributes
	 *            the attributes whose codecs to concatenate
	 * @return the concatenated codecs
	 */
	public static CodecInfo[] concatenateCodecs(final DatasetAttributes attributes) {

		return concatenateCodecs(
				attributes.getDatasetCodecInfos(),
				attributes.getBlockCodecInfo(),
				attributes.getDataCodecInfos());
	}

	/**
	 * Flattens the given codecs into a single array, in the order {@link #parse} expects:
	 * dataset ({@code array -> array}) codecs, then the block ({@code array -> bytes})
	 * codec, then data ({@code bytes -> bytes}) codecs.
	 * <p>
	 * This is the inverse of {@link #parse}; the two must be kept in step.
	 *
	 * @param datasetCodecInfos
	 *            the dataset codecs, may be null
	 * @param blockCodecInfo
	 *            the block codec
	 * @param dataCodecInfos
	 *            the data codecs, may be null
	 * @return the concatenated codecs
	 */
	public static CodecInfo[] concatenateCodecs(
			final DatasetCodecInfo[] datasetCodecInfos,
			final BlockCodecInfo blockCodecInfo,
			final DataCodecInfo[] dataCodecInfos) {

		final int numDataset = datasetCodecInfos == null ? 0 : datasetCodecInfos.length;
		final int numData = dataCodecInfos == null ? 0 : dataCodecInfos.length;

		final CodecInfo[] codecs = new CodecInfo[numDataset + 1 + numData];
		if (numDataset > 0)
			System.arraycopy(datasetCodecInfos, 0, codecs, 0, numDataset);

		codecs[numDataset] = blockCodecInfo;

		if (numData > 0)
			System.arraycopy(dataCodecInfos, 0, codecs, numDataset + 1, numData);

		return codecs;
	}
}
