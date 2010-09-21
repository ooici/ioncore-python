import datetime
import uuid
import simplejson as json

# Anything after this token in a log line is considered a parsable event
CEI_EVENT_SEPARATOR = "CEI_EVENT_JSON:"

KEY_SOURCE = "eventsource"
KEY_NAME = "eventname"
KEY_UNIQUEKEY = "uniquekey"
KEY_EXTRA = "extra"
KEY_STAMP = 'timestamp'
KEY_STAMP_YEAR = 'year'
KEY_STAMP_MONTH = 'month'
KEY_STAMP_DAY = 'day'
KEY_STAMP_HOUR = 'hour'
KEY_STAMP_MINUTE = 'minute'
KEY_STAMP_SECOND = 'second'
KEY_STAMP_MICROSECOND = 'microsecond'

# ------------------ EVENT CREATION --------------------------------

def event(source, name, logger, extra=None):
    """Record an event for later retrieval.
    @param source The event source, can use this for grouping events.
    @param name The event name.
    @param logger logger must be provided from outside.
    @param extra Some opaque dict that you will consult after parsing.
    """
    if not logger:
        raise Exception("logger is required")
    logger.warning(event_logtxt(source, name, extra=extra))

def event_logtxt(source, name, extra=None):
    """Same as the 'event' function, but you control where text is recorded.
    """
    json = event_json(source, name, extra=extra)
    return "%s %s" % (CEI_EVENT_SEPARATOR, json)

def event_json(source, name, extra=None):
    """Event text without any keyword to help parser: you are on your own.
    """
    return json.dumps(_event_dict(source, name, extra=extra))

def _event_dict(source, name, extra=None):
    if not source:
        raise Exception("event source is required")
    if not name:
        raise Exception("event name is required")
    _valid(source)
    _valid(name)
    _valid_dict(extra)
        
    uniquekey = str(uuid.uuid4())
    timestamp = _timestamp_to_dict(_timestamp_now())
    
    ev = {KEY_SOURCE: source,
          KEY_NAME: name,
          KEY_UNIQUEKEY: uniquekey,
          KEY_STAMP: timestamp,
          KEY_EXTRA: extra}

    return ev

def _valid(string):
    if not string:
        return
    if string.rfind("\n") >= 0:
        raise Exception("Cannot contain newline: %s" % string)
        
def _valid_dict(adict):
    if not adict:
        return
    # usually frowned upon, but feel strictness will do more good than harm here
    if not isinstance(adict, dict):
        raise Exception("the extra portion of an event needs to be a dict")
        
    # only checks the first level for now..
    for k in adict.keys():
        if isinstance(k, int):
            raise Exception("the json module won't support integer keys")
    

# ------------------ EVENT HARVESTING --------------------------------

class CEIEvent:
    """Convenience class for a parsed event.
    """
    
    def __init__(self, source, name, key, timestamp, extra):
        self.source = source
        self.name = name
        self.key = key
        self.timestamp = timestamp
        self.extra = extra

def events_from_file(path, sourcefilter=None, namefilter=None):
    """Return list of CEIEvent instances found in a file.
    @param sourcefilter scope list to events with source having this prefix
    @param namefilter scope list to events with name having this prefix
    """
    
    events = []
    for line in open(path):
        ev = _event_from_logline(line)
        if not ev:
            continue
        if sourcefilter and not ev.source.startswith(sourcefilter):
            continue
        if namefilter and not ev.name.startswith(namefilter):
            continue
        events.append(ev)
    return events

def _event_from_logline(log_line):
    if not log_line:
        return None
    idx = log_line.rfind(CEI_EVENT_SEPARATOR)
    if idx < 0:
        return None
    parts = log_line.split(CEI_EVENT_SEPARATOR)
    if len(parts) != 2:
        return None
    return _event_from_json(parts[1])

def _event_from_json(json_string):
    jsondict = json.loads(json_string)
    source = jsondict[KEY_SOURCE]
    name = jsondict[KEY_NAME]
    key = jsondict[KEY_UNIQUEKEY]
    extra = jsondict[KEY_EXTRA]
    stampdict = jsondict[KEY_STAMP]
    timestamp = _dict_to_timestamp(stampdict)
    return CEIEvent(source, name, key, timestamp, extra)


# ------------------ TIMESTAMP --------------------------------

# measurements that happen around a leap second could be unusable
def _timestamp_now():
    return datetime.datetime.utcnow()

def _timestamp_to_dict(dt_inst):
    ts = {}
    ts[KEY_STAMP_YEAR] = dt_inst.year
    ts[KEY_STAMP_MONTH] = dt_inst.month
    ts[KEY_STAMP_DAY] = dt_inst.day
    ts[KEY_STAMP_HOUR] = dt_inst.hour
    ts[KEY_STAMP_MINUTE] = dt_inst.minute
    ts[KEY_STAMP_SECOND] = dt_inst.second
    ts[KEY_STAMP_MICROSECOND] = dt_inst.microsecond
    return ts

def _dict_to_timestamp(jsondict):
    return datetime.datetime(jsondict[KEY_STAMP_YEAR],
                             jsondict[KEY_STAMP_MONTH],
                             jsondict[KEY_STAMP_DAY],
                             jsondict[KEY_STAMP_HOUR],
                             jsondict[KEY_STAMP_MINUTE],
                             jsondict[KEY_STAMP_SECOND],
                             jsondict[KEY_STAMP_MICROSECOND])
