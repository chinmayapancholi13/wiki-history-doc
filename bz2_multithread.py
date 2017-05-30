import sys
import os
import time
import bz2
import urllib2

BZ2_CHUNK = 10*1000*1024
STR_CHUNK = 40*1000*1024

#url = 'http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles26.xml-p42567204p42663461.bz2'
#url = 'http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-meta-current1.xml-p10p30303.bz2'

sys.argv = ["",""]
sys.argv[1] = 'enwiki-latest-pages-articles26.xml-p42567204p42663461.bz2'
#sys.argv[1] = 'extracted/AA/wiki_00.bz2'
#sys.argv[1] = 'enwiki-latest-pages-meta-current1.xml-p10p30303.bz2'
#sys.argv[1] = 'http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-meta-current1.xml-p10p30303.bz2'
if len(sys.argv) > 1:
	url = sys.argv[1]

http = False
if len(url.split('://')) > 1:
	http = True

#parse one page using split(). Fast!
def parser(buf):
	keys_h_t = {'title': ('<title>', '</title>'), 
		'ns': ('<ns>', '</ns>'),
		'id': ('<id>', '</id>'), 
		'text': ('<text ', '</text>')}
	d = {k: '' for k in keys_h_t.keys()}
	for key, (head, tail) in keys_h_t.items():
		#get rid of front
		lst = buf.split(head)
		if len(lst) > 1:
			#get rid of tail
			lst = lst[1].strip().split(tail)
			if len(lst)> 1:
				d[key] = lst[0]
	return d

#parse one page using regex. Slow!
import re
regex = ["<title>[\S\s]+?</title>", "<id>[\S\s]+?</id>", "<ns>[\S\s]+?</ns>","<text[\S\s]+?</text>"]
def parser1(buf):
	keys = ['title',  'id', 'ns', 'text']
	d = {k: '' for k in keys}
	for i, k in enumerate(keys): 
		#print i,k, regex[i], buf[:100]
		lst = re.findall(regex[i], buf, re.IGNORECASE)
		if len(lst) > 0:
			d[k] = lst[0]
	return d

#support getPagesByUrl() and getPagesByPath()
def getPages(chunk):
	lst = chunk.split("<page>")
#	pages = map(lambda page: parser1(page), lst)
	pages = map(lambda page: parser(page), lst)
	return pages

#parse page with url input
def getPagesByUrl(url, bytes = BZ2_CHUNK):

	decompressor = bz2.BZ2Decompressor()
	req = urllib2.urlopen(url)
	pages = []
	b = 0
	while True:
		t1 = time.time()
		chunk = req.read(bytes)
		if not chunk:
		    break
		text = decompressor.decompress(chunk)
		pages += getPages(text)
		b += len(text)
		print "%.6f"%(time.time()-t1) ,len(pages), b
	req.close()

	return pages, b

#parse page with file_path input
def getPagesByPath(path, bytes = STR_CHUNK):

	fd = bz2.BZ2File(path, 'rb')
	pages = []
	b = 0
	while True:
		t1 = time.time()
		chunk = fd.read(bytes)
		if not chunk:
		    break
		pages += getPages(chunk)
		b += len(chunk)
		print "%.6f"%(time.time()-t1) ,len(pages), b
	fd.close()

	return pages, b

#multithreading parsing
import threading
import Queue
def threadwork(chunk, out_queue):
	lst = chunk.split("<page>")
#	pages = map(lambda page: parser1(page), lst)
	pages = map(lambda page: parser(page), lst)
	return out_queue.put(pages )

#multithreading parsing pages with a file_path input
def getPagesByPath_multithread(path, bytes = STR_CHUNK):
	t0 = time.time()
	my_queue = Queue.Queue()
	thread_list = []
	n_thread = 0

	fd = bz2.BZ2File(path, 'rb')
	b = 0
	while True:
		chunk = fd.read(bytes)
		if not chunk:
		    break
		t = threading.Thread(target=threadwork, args=(chunk ,my_queue))
		thread_list.append(t)
		b += len(chunk)
		print "thread: %d start time: %.6f, buf: %d"%(n_thread, time.time()-t0 ,b)
		n_thread += 1
		t.start()
	fd.close()
	for t in thread_list:
		t.join()
	pages = []
	for i in xrange(n_thread):
		pages += my_queue.get()
		print "thread: %d done time: %.6f, page_len: %d"%(i, time.time()-t0 ,len(pages))

	return pages, b

#multithreading parsing pages with an url input
def getPagesByUrl_multithread(url, bytes = BZ2_CHUNK):
	t0 = time.time()
	my_queue = Queue.Queue()
	thread_list = []
	n_thread = 0

	decompressor = bz2.BZ2Decompressor()
	req = urllib2.urlopen(url)
	b = 0
	while True:
		chunk = req.read(bytes)
		if not chunk:
		    break
		text = decompressor.decompress(chunk)
		t = threading.Thread(target=threadwork, args=(text ,my_queue))
		thread_list.append(t)
		b += len(text)
		print "thread: %d start time: %.6f, buf: %d"%(n_thread, time.time()-t0 ,b)
		n_thread += 1
		t.start()
	req.close()
	for t in thread_list:
		t.join()
	pages = []
	for i in xrange(n_thread):
		pages += my_queue.get()
		print "thread: %d done time: %.6f, page_len: %d"%(i, time.time()-t0 ,len(pages))

	return pages, b


t0 = time.time()

if http:
	#reading data from url
#	pages, b = getPagesByUrl(url)
	pages, b = getPagesByUrl_multithread(url)
else:
	#reading data from local
#	pages, b = getPagesByPath(url)  
	pages, b = getPagesByPath_multithread(url)
	len1 = os.path.getsize(url)
	print "file_size:",len1

print pages[0]
print pages[3]

print "total time: %.6f"%(time.time()-t0), len(pages), b
