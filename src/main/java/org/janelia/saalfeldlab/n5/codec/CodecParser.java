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

	private static CodecInfo[] concatenateCodecs(DatasetAttributes attributes) {

		final CodecInfo[] codecs = new CodecInfo[attributes.getDataCodecInfos().length + 1];
		codecs[0] = attributes.getBlockCodecInfo();
		System.arraycopy(attributes.getDataCodecInfos(), 0, codecs, 1, attributes.getDataCodecInfos().length);
		return codecs;
	}
}
