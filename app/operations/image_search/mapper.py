#! /usr/bin/env python

import sys
import numpy
import cv2
#from cv2 import cv
import random
from flask import current_app

try:
    from hdfs import InsecureClient
except:
    pass
   
def search_img_desc(orb, desc, img_data):

	nparr = numpy.fromstring(img_data, numpy.uint8)
	img2 = cv2.imdecode(nparr, cv2.IMREAD_COLOR) # cv2.IMREAD_COLOR in OpenCV 3.1

	kp2, desc2 = orb.detectAndCompute(img2, None)
	bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)

        # Match descriptors.
	matches = bf.match(desc,desc2)
	return len(matches)

def search_img(src, key_value):
	data = []
	hdfs = InsecureClient(current_app.config['WEBHDFS_ADDR'], user=current_app.config['WEBHDFS_USER'])
	with hdfs.read(src) as reader:
		data = reader.read()

	nparr = numpy.fromstring(data, numpy.uint8)
	img1 = cv2.imdecode(nparr, cv2.IMREAD_COLOR) # cv2.IMREAD_COLOR in OpenCV 3.1
	
	orb = cv2.ORB_create()
	# find the keypoints and descriptors with SIFT
	kp1, desc = orb.detectAndCompute(img1, None)
	
	m={}
	for key in key_value:
		m[key]=search_img_desc(orb, desc, key_value[key])

	return m

def read_raw_bytes(input):
    length_bytes = input.read(4)
    if len(length_bytes) < 4:
        return None
    length = 0
    for b in length_bytes:
        length = (length << 8) + ord(b)
    return input.read(length)

def write_raw_bytes(output, s):
    length = len(s)
    length_bytes = []
    for _ in range(4):
        length_bytes.append(chr(length & 0xff))
        length = length >> 8
    length_bytes.reverse()
    for b in length_bytes:
        output.write(b)
    output.write(s)

def read_keys_and_values(input):
    d = {}
    while True:
        key = read_raw_bytes(input)
        if key is None: break
        value = read_raw_bytes(input)
        d[key] = value
    return d

def write_keys_and_values(output, d):
    for key in d:
        write_raw_bytes(output, key)
        write_raw_bytes(output, str(d[key]))

if __name__ == "__main__":
	key_values = read_keys_and_values(sys.stdin)
	matches = search_img(sys.argv[1], key_values)
	write_keys_and_values(sys.stdout, matches)
