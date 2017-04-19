from paxos_utils import json_spaceless_dump, json_set_serializable_load
import sys

if __name__ == '__main__':
	log_file = sys.argv[1]
	shard_id = log_file[log_file.find("shard") + 5]
	data = {}
	with open(log_file, 'r') as f:
		for line in f:
			line = line.rstrip('\n')
			request, result = line.split('\t')
			result = result.split(' ', 1)[1]
			type_of_req, rest = tuple(request.split(' ', 1))

			if type_of_req == 'InitData':
				data = json_set_serializable_load(rest)
				assert result == shard_id

			elif type_of_req == 'AddShard':
				begin, end, others = tuple(rest.split(' ', 2))
				begin, end = int(begin), int(end)
				transfer_data = {}
				for key, value in data.iteritems():
					if int(key) >= begin and int(key) < end:
						transfer_data[key] = value
				assert json_spaceless_dump(transfer_data) == result

			elif type_of_req == 'Put':
				key, value = tuple(rest.split(' ', 1))
				data[key] = value
				assert result == '1'

			elif type_of_req == 'Get':
				key, trash = tuple(rest.split(' ', 1))
				if key in data:
					assert result == data[key]
				else:
					assert result == 'No such key'

			elif type_of_req == 'Delete':
				key, trash = tuple(rest.split(' ', 1))
				if key in data:
					del data[key]
					assert result == '1'
				else:
					assert result == 'No such key'
	print "Correct!"