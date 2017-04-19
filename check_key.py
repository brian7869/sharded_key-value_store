import sys

if __name__ == '__main__':
	folder = sys.argv[1]
	num_shard = int(sys.argv[2])
	num_replica = int(sys.argv[3])
	out = open('log/exec_by_key.out', 'w')
	for i in xrange(num_shard):
		for j in xrange(num_replica):
			for key in xrange(1,16):
				f = open(folder+'shard{}_replica{}.log'.format(i,j), 'r')
				for line in f:
					if ' '+str(key)+' ' in line or '"'+str(key)+'"' in line:
						out.write(line)