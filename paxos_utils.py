import json, pickle, hashlib, socket, time
from config import HASH_FUNC, NUM_HEXDIGITS, THRESHOLD

class PythonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}

def as_python_object(dct):
    if '_python_object' in dct:
        return pickle.loads(str(dct['_python_object']))
    return dct

def json_spaceless_dump(obj):
	return json.dumps(obj, separators=(',', ':'), cls=PythonObjectEncoder)

def json_set_serializable_load(obj):
	return json.loads(obj, object_hook=as_python_object)

def get_hash_value(key):
    hash_func = hashlib.new(HASH_FUNC)
    hash_func.update(key)
    return int(hash_func.hexdigest()[:NUM_HEXDIGITS], 16)

def within_the_range(begin, end, hash_value):
    return hash_value >= begin and hash_value < end

def send_message(host, port, message, random):
    if message.find("Heartbeat") != -1 or random.random() > THRESHOLD:
        if message.find("Heartbeat") == -1:
            if message.find("YouAreLeader") != -1: 
                print "send '{}' to {}:{}".format(message[:message.find('{')], host, str(port))
            else:
                print "send '{}' to {}:{}".format(message, host, str(port))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, int(port)))
            sock.sendall(message)
            sock.close()
        except Exception:
            pass
    else:
        if message.find("YouAreLeader") != -1: 
            print "drop '{}' to {}:{}".format(message[:message.find('{')], host, str(port))
        else:
            print "drop '{}' to {}:{}".format(message, host, str(port))

def send_message_without_failure(host, port, message):
    print "send '{}' to {}:{}".format(message, host, str(port))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, int(port)))
        sock.sendall(message)
        sock.close()
    except Exception:
        pass