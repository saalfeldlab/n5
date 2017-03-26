##^HDF5

HDF5 is a great format that provides a wealth of conveniences that I do not want to miss.  It's inefficiency for parallel writing, however, make 
^HDF5 is a library whose sole purpose is to overcome some of the most annoying limitations of HDF5 by not using it.  Instead, it uses the native filesystem of the target platform and JSON files to specify basic and custom meta-data.  It aims at preserving the convenience where possible but doesn't try too hard to be a full replacement.
