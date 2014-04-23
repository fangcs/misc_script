import sys
import csv
import operator
import datetime

arg_len=len(sys.argv)
if arg_len != 2:
	print 'usage: python twitter_2_GPS.py input_file'
	sys.exit(1)

input_file = open(sys.argv[1])
split_num = 1000

temp_files = []
for i in range(0, split_num):
	temp_files.append(open(sys.argv[1] + '_temp_' + str(split_num) + '_' + str(i), 'w'))

linenum = long(0)
start_time=datetime.datetime.now()
while 1:
	lines = input_file.readlines(10000000)
	if not lines:
		break
	for line in lines:
		temps = line.split()
		src = long(temps[1])
		dst = long(temps[0])
		temp_file = temp_files[src % split_num]
		temp_str = '%d\t%d\n' % (src, dst)
		temp_file.write(temp_str)

	linenum = linenum + len(lines)
	output_str = 'read %d lines' % (linenum)
	print output_str
input_file.close()

part1_time=datetime.datetime.now()
print 'write temp file cost {0} seconds.'.format((part1_time - start_time).seconds)

for i in range(0, split_num):
	temp_files[i].close();

output_file = open(sys.argv[1] + '_GPS', 'w')

for i in range(0, split_num):
	unsorted_filename = sys.argv[1] + '_temp_' + str(split_num) + '_' + str(i) 
	unsorted_file = open(unsorted_filename);
	unsorted_data = csv.reader(unsorted_file, delimiter='\t')
	sorted_data = sorted(unsorted_data, key=lambda x: (long(x[0]), long(x[1])))
	first = True
	current_src = long(0)
	temp_array = []
	for row in sorted_data:
		this_src = long(row[0])
		if current_src == this_src:
			temp_array.append(long(row[1]))
		elif first == True:
			current_src = this_src
			temp_array.append(long(row[1]))
			first = False
		else:
			output_file.write(str(current_src))
			for j in temp_array:
				output_file.write(" ")
				output_file.write(str(j))
			output_file.write('\n')
			current_src = this_src
			temp_array = []
			temp_array.append(long(row[1]))
	if len(temp_array) > 0:
		output_file.write(str(current_src))
		for j in temp_array:
			output_file.write(" ")
			output_file.write(str(j))
		output_file.write('\n')
	unsorted_file.close()

end_time=datetime.datetime.now()
output_file.close()
print 'total cost {0} seconds.'.format((end_time - start_time).seconds)
