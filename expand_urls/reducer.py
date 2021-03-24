#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import csv
import re
#from urllib import unquote

current_url = None
current_ids = ""
url = None
counter = 1

output_file = open('./urls_todo_sorted_grouped.csv', 'w')

with open('./urls_todo_sorted.csv') as csvfile:
     lines = csv.reader(csvfile, delimiter=',', quotechar='"')
     for line in lines:
        url = line[0].strip()
        url = url.strip('"')
        url = re.sub(r'[^\w.#?]*$','',url) # remove trailing comma's, parenthesis, etc.

        id = line[1].strip()
        
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

output_file.close()
