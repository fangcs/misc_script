import sys
import os
import datetime

#1.test the argv[1]
arg_len=len(sys.argv)
#print arg_len
if arg_len != 3:
	print 'usage: python bloat_netflix.py input_file bloat_multiplier'
	sys.exit(1)
#for i in range(0, len(sys.argv)):
#	print "parameter", i, sys.argv[i]
bloat_multiplier=long(sys.argv[2])
#print bloat_multiplier

if bloat_multiplier == 1:
	print 'no need to bloat the size'
	sys.exit(1)

input_file = open(sys.argv[1])
temp_files = []
for i in range(0, bloat_multiplier):
	temp_files.append(open(sys.argv[1] + '_temp_' + sys.argv[2] + '_' + str(i), 'w'))
	#print 'open output files'+str(i)
output_file = open(sys.argv[1] + '_' + sys.argv[2], 'w')
linenum=long(0)
init_tag=0
user_size=long(0)
movie_size=long(0)
edge_size=long(0)
report_level=long(0)
start_time=datetime.datetime.now()
while 1:
	lines = input_file.readlines(100000)
	if not lines:
		break

	for line in lines:
	#print line
	#print linenum
		linenum = linenum + long(1)
		if line[0:1] == '%':
			output_file.write(line)
		elif init_tag == 0:
			init_tag = 1
			temps = line.split()
			user_size = long(temps[0])
			movie_size = long(temps[1])
			edge_size = long(temps[2])
			print 'original user sum: {0}'.format(user_size)
			print 'original movie sum: {0}'.format(movie_size)
			print 'original edge sum: {0}'.format(edge_size)
			report_level=edge_size/1000*10
			if report_level < 100000:
				report_level = 100000
			size_str = '%d %d %d\n' % (user_size*bloat_multiplier, movie_size*bloat_multiplier, edge_size*bloat_multiplier)
		#	size_str=str(user_size*bloat_multiplier) \
		#		+ ' ' + str(movie_size*bloat_multiplier) \
		#		+ ' ' + str(edge_size*bloat_multiplier) + '\n'
			output_file.write(size_str)
		else:
			temps = line.split()
			user_id = long(temps[0])
			movie_id = long(temps[1])
			#score = long(temps[2])
			for i in range(0, bloat_multiplier):
				temp_str = '%d %d  %s\n' % (user_id + long(i) * user_size \
						, movie_id + long(i) * movie_size \
						, temps[2])
		#		temp_str = str(user_id + long(i*bloat_multiplier)*user_size) + ' ' \
		#			+ str(movie_id + long(i*bloat_multiplier)*movie_size) + '  ' \
		#			+ temps[2] + '\n'
				temp_files[i].write(temp_str)
		if (report_level > 0) and (linenum % report_level == 0):
			print 'progress: {0} / {1}'.format(linenum, edge_size)
        #pass # do something

input_file.close()
for i in range(0, bloat_multiplier):
	temp_files[i].close()
		
for i in range(0, bloat_multiplier):
	temp_file = open(sys.argv[1] + '_temp_' + sys.argv[2] + '_' + str(i), 'r')
	print 'merge temp file: '+str(i)
	while 1:
		lines = temp_file.readlines(100000)
		if not lines:
			break
		output_file.writelines(lines)
	temp_file.close()

output_file.close()
end_time=datetime.datetime.now()
print 'total cost {0} seconds.'.format((end_time - start_time).seconds)
