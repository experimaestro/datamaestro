from pathlib import Path
from struct import Struct
from . import File


class IDX(File):
    """IDX File format

    The IDX file format is a simple format for vectors and multidimensional matrices of various numerical types.

    The basic format is:

    magic number
    size in dimension 0
    size in dimension 1
    size in dimension 2
    .....
    size in dimension N
    data

    The magic number is an integer (MSB first). The first 2 bytes are always 0.

    The third byte codes the type of the data:
    0x08: unsigned byte
    0x09: signed byte
    0x0B: short (2 bytes)
    0x0C: int (4 bytes)
    0x0D: float (4 bytes)
    0x0E: double (8 bytes)

    The 4-th byte codes the number of dimensions of the vector/matrix: 1 for vectors, 2 for matrices....

    The sizes in each dimension are 4-byte integers (MSB first, high endian, like in most non-Intel processors).

    The data is stored like in a C array, i.e. the index in the last dimension changes the fastest.
    """

    MAGIC_NUMBER = Struct(">HBB")
    DIM = Struct(">I")

    def data(self):
        """Returns the tensor"""
        import numpy as np

        with self.open("rb") as fp:
            zero, magic, size = IDX.MAGIC_NUMBER.unpack_from(
                fp.read(IDX.MAGIC_NUMBER.size)
            )
            if zero != 0:
                raise IOError("File format not IDX (the two first bytes are not zero)")

            if magic == 0x08:
                dtype = ">B"
            elif magic == 0x09:
                dtype = ">b"
            else:
                raise IOError("Magic number {} not recognized in IDX file", magic)

            shape = [IDX.DIM.unpack_from(fp.read(IDX.DIM.size))[0] for i in range(size)]

            size = np.prod(shape)
            # Could use np.fromfile... if it were not broken - see https://github.com/numpy/numpy/issues/7989
            data = np.frombuffer(fp.read(), dtype=dtype, count=size)
            data = data.reshape(shape, order="C")
        return data
