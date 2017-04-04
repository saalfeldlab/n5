##N5

HDF5 is a great format that provides a wealth of conveniences that I do not want to miss.  It's inefficiency for parallel writing, however, limit its applicability for handling of very large n-dimensional images.
N5 is a library whose sole purpose is to overcome this limitation of HDF5 by not using it.  Instead, it uses the native filesystem of the target platform and JSON files to specify basic and custom meta-data as attributes.  It aims at preserving the convenience where possible but doesn't try too hard to be a full replacement.
Please do not take this project too seriously, we will see where it will get us and report back when more data is available.
