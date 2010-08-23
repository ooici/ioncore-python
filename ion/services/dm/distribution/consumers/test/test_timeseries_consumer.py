#!/usr/bin/env python





ds = DatasetType(name='SimpleGridData')
    
    g = GridType(name='grid')
    data = numpy.arange(24.)
    data.shape = (4, 2, 3)
    # The name in the dictionary must match the name in the basetype
    g['a'] = BaseType(name='a', data=data, shape=data.shape, type=Float32, dimensions=('time', 'x', 'y'))
    g['time'] = BaseType(name='time', data=numpy.arange(4.), shape=(4,), type=Float64)
    g['x'] = BaseType(name='x', data=numpy.arange(2.), shape=(2,), type=Float64)
    g['y'] = BaseType(name='y', data=numpy.arange(3.), shape=(3,), type=Float64)

    ds[g.name]=g
    return ds