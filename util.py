import sys
import cPickle
from datetime import datetime
from time import mktime


def show_timing(count, start, ts):
    tot_per_sec = int(count / (ts - start))
    secs_in_day = 86400
    tot_per_day = secs_in_day * tot_per_sec
    if tot_per_day > 0:
        print(str(count) + " total (" + str(tot_per_sec) + "/s)")
    sys.stdout.flush()


def serialize(filename, obj):
    pickle_file = open(filename, 'wb')
    cPickle.dump(obj, pickle_file)
    pickle_file.close()


def format_time_in_seconds(datetime_object):
    return int(mktime(
        datetime.strptime(datetime_object, '%a %b %d %H:%M:%S %Y').timetuple()))


def deserialize(filename):
    pickle_file = open(filename, 'rb')
    obj = cPickle.load(pickle_file)
    pickle_file.close()
    return obj


def tabs_at_beginning(line):
    return len(line) - len(line.lstrip('\t'))


def write_object(filename, obj):
    input_filename = open(filename, "w")
    input_filename.write(obj)
    input_filename.close()
