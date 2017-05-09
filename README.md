# N5

N5 is a library to store large chunked n-dimensional tensors, and arbitrary meta-data in a hierarchy of groups similar to HDF5.  Other than HDF5, an N5 group is not a single file but simply a directory on the file system.  Meta-data is stored as a JSON file per each group/ directory.  Tensor datasets can be chunked and chunks are stored as individual files.  This enables parallel reading and writing on a cluster.  At this time, N5 supports:

* arbitrary group hierarchies
* arbitrary meta-data stored as JSON
* chunked n-dimensional tensor datasets
* value-datatypes: [u]int8, [u]int16, [u]int32, [u]int64, float32, float64
* compression: raw, gzip, bzip2, xz

Chunked datasets can be sparse, i.e. empty chunks do not need to be stored.

## Specifications

1. All directories of the file system are N5 groups.
2. A JSON file `attributes`.json in a directory contains arbitrary attributes.
3. A dataset is a group with the mandatory attributes:
   * dimensions (e.g. [100, 200, 300]),
   * blockSize (e.g. [64, 64, 64]),
   * dataType (one of {uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64})
   * compressionType (one of {raw, bzip2, gzip, xz}).
4. Chunks are stored in a directory hierarchy that enumerates their position in the chunk grid.
5. All chunks of a chunked dataset have the same size except for end-chunks that may be smaller (thereofore 6.)
6. Each chunk file contains the block size of the chunk in the first 4 * n bytes

## Disclaimer

HDF5 is a great format that provides a wealth of conveniences that I do not want to miss.  It's inefficiency for parallel writing, however, limit its applicability for handling of very large n-dimensional data.

N5 uses the native filesystem of the target platform and JSON files to specify basic and custom meta-data as attributes.  It aims at preserving the convenience of HDF5 where possible but doesn't try too hard to be a full replacement.
Please do not take this project too seriously, we will see where it will get us and report back when more data is available.
