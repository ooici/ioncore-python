
import msgpack

from ion.play.datastore import datatype
from ion.play.datastore.encoders import gpb

person = {
        'name':datatype.String(u'joe'),
        'email':datatype.String(u'joe@home.net'),
        'weight':datatype.Float(170.5),
        }

def google_encoder():
    person_dataobject = datatype.DataObject(person)

    print "\nA DataObject instance of the person structure"
    print person_dataobject
    print 'len', len(str(person_dataobject))

    print '\nmsgpack'
    msgpack_person = msgpack.packb({
        'name':u'joe',
        'email':u'joe@home.net',
        'weight':170.5,
        })
    print repr(msgpack_person)
    print 'len', len(msgpack_person)

    encoder = gpb.Encoder()
    encoded_person_bytes = encoder.encode(person_dataobject)

    print "\nThe encode DataObject byte string"
    print repr(encoded_person_bytes)
    print 'len', len(encoded_person_bytes)

    decoded_person = encoder.decode(encoded_person_bytes)

    print "\nA new DataObject created by decoding the byte string"
    print decoded_person

def multi_dataobject():
    robot = datatype.DataObject({
            'name':datatype.String(u'Earl'),
            'id':datatype.Int(123445),
            'mind':datatype.DataObject({
                    'name':datatype.String(u'Quincy'),
                    'favorite_color':datatype.String(u'orange'),
                    'number':datatype.Int(42),
                    'seconds':datatype.Long(24123445524242433),
                    })
            })

    print robot

    encoder = gpb.Encoder()

    encoded_robot = encoder.encode(robot)

    decoded_robot = encoder.decode(encoded_robot)

    print decoded_robot




if __name__ == "__main__":
    print '\nExample 1'
    google_encoder()

    print '\n\n\n'

    print 'Example 2'
    multi_dataobject()
