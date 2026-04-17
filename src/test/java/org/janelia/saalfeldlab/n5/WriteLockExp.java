package org.janelia.saalfeldlab.n5;

import java.util.Arrays;


import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.universe.N5Factory;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

public class WriteLockExp {

    long[] dimensions = new long[]{1024, 1024};
    int[] shardSize = {1024, 1024};
    int[] chunkSize = {64, 64};
    private String root;
    private int shardIdx;

    int[] data;

    public WriteLockExp(String root, int shardIdx) {

        this.root = root;
        this.shardIdx = shardIdx;
    }

    public static void main(String[] args) {

        String root = args[0];
        int shardIdx = Integer.parseInt(args[1]);

        WriteLockExp exp = new WriteLockExp( root, shardIdx );
        exp.run();
    }

    public void run() {

        System.out.println("start");
        int N = 100_000;
        final int chunkN = chunkSize[0] * chunkSize[1];
        data = data(chunkN, shardIdx+1);

        final IntArrayDataBlock block =  new IntArrayDataBlock(chunkSize, new long[]{shardIdx, shardIdx}, data);

        try (N5Writer n5 = N5Factory.createWriter(root)) {

            final DatasetAttributes attrs = getOrCreateDataset(n5);
            for (int i = 0; i < N; i++) {

                if( i % 1000 == 0)
                    System.out.println("iter: " + i);

//				n5.writeRegion("", attrs, new long[]{0,0}, dimensions, blockSupplier(shardIdx, chunkSize, chunkN), true);
                n5.writeChunks("", attrs, block);

            }
        } catch (N5Exception e) {
            e.printStackTrace();
        }
        System.out.println("done");
    }

    public DatasetAttributes getOrCreateDataset(N5Writer n5) {

        ZarrV3DatasetAttributes tmpAttrs = ZarrV3DatasetAttributes.builder(dimensions, DataType.INT32)
                .shardShape(shardSize)
                .blockSize(chunkSize)
                .compression(new ZstandardCompression())
                .build();

        final DatasetAttributes existingAttrs = n5.getDatasetAttributes("");
        if( existingAttrs != null )
            return existingAttrs;

        return n5.createDataset("", tmpAttrs);
    }

    DataBlockSupplier<int[]> blockSupplier(final int i, final int[] chunkSize, final int chunkN) {

        return new DataBlockSupplier<int[]>() {

            @Override
            public DataBlock<int[]> get(long[] gridPos, DataBlock<int[]> existingDataBlock) {

                if (gridPos[0] == i && gridPos[1] == i)
                    return new IntArrayDataBlock(chunkSize, gridPos, data);
                else
                    return null;
            }
        };
    }

    static int[] data(final int chunkN, final int value) {
        final int[] data = new int[chunkN];
        Arrays.fill(data, value);
        return data;
    }

}