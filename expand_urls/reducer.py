#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
#from urllib import unquote

current_url = None
current_ids = ""
url = None
counter = 1


# input comes from csv
input_file = open('./urls_todo.csv', 'r')
lines = input_file.readlines()

output_file = open('./urls_todo_grouped.csv','w')

# loop over lines
for line in lines:
    # remove leading and trailing whitespace
    line = line.strip()

    # skip empty lines
    if not line:
        print("SKIP: empty line")
        continue

    # skip SQL residue
    #if not line.startswith("http"):
    #    print ("SKIP: line does not start with http -> %s" % (url))
    #    continue

    # parse the input we got from mapper
    try:
        url, id = line.split(',', 1)
    except ValueError:
        print("Could not split %s" % line)
        continue

    # clean split
    url = url.strip('"')
    id = id.strip()

    # unencode
    # url = unquote(unquote(url))

    # skip wrong URLs
    if not url.startswith("http://"):
        if not url.startswith("https://"):
            print("SKIP: url does not start with http:// or https:// -> %s\t%s " % (id, url))
            continue

    # this IF-switch only works because the data is already supposed to be sorted
    # by key (here: url) before it is passed to the reducer
    if current_url == url:
        #print "."
        if not current_ids:
            current_ids = id
            counter = 1
        else:
            current_ids = current_ids + ", " + id
            counter += 1
    else:
        if current_url:
            if not current_ids:
                current_ids = id
                counter = 1
            # write result to STDOUT
            #print '%s\t%s\t%s' % (current_url, counter, current_ids)
            # write result to file
            output_file.write('%s\t%s\t%s\n' % (current_url, counter, current_ids))
        current_ids = ""
        current_url = url
        counter = 0

# do not forget to output the last word if needed!
if current_url == url:
    #print '%s\t%s\t%s' % (current_url, counter, current_ids)
    output_file.write('%s\t%s\t%s\n' % (current_url, counter, current_ids))

input_file.close()
output_file.close()
