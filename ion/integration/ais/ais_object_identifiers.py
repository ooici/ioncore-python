#
# AIS ObjectIDs
#
from ion.core.object import object_utils

aisRequestMsgType = object_utils.create_type_identifier(object_id=9001, version=1)
aisResponseMsgType = object_utils.create_type_identifier(object_id=9002, version=1)

