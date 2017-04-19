import sys, os

if __name__ == '__main__':
	folder = sys.argv[1]
	num_shard = int(sys.argv[2])
	num_replica = int(sys.argv[3])
	out = open('log/exec_by_key.out', 'w')
	for i in xrange(num_shard):
		largest_size = 0
		largest_id = -1
		for j in xrange(num_replica):
			stat_info = os.stat(folder+'shard{}_replica{}.log'.format(i,j))
			size = stat_info.st_size
			if size > largest_size:
				largest_id = j
				largest_size = size

		for key in xrange(16):
			f = open(folder+'shard{}_replica{}.log'.format(i,largest_id), 'r')
			for line in f:
				if ' '+str(key)+' ' in line:
					out.write(line)