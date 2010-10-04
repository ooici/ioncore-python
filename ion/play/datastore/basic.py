"""
inspired by the pydap model module
"""

def TypeFactory(name, typenumber):
    return type(name, (object,), {'typenumber':typenumber, 'descriptor': name})


Float64 = TypeFactory('Float64', 1)
Float32 = TypeFactory('Float32', 2)
Int32 = TypeFactory('Int32', 3)
Int16 = TypeFactory('Int16', 4)
UInt32 = TypeFactory('UInt32', 5)
UInt16 = TypeFactory('UInt16', 6)
Byte = TypeFactory('Byte', 7)
Bytes = TypeFactory('Bytes', 8)
String = TypeFactory('String', 9)
Boolean = TypeFactory('Boolean', 10)

basetypes = [Float64, Float32, Int32, Int16, UInt32, UInt16, Byte, Bytes,
        String, Boolean]

class CompositeType(object):

    def __init__(self, name, type):
        self.name = name
        self.type = type
