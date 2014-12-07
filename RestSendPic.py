import sys,os
import requests
import json

filename = sys.argv[1]
fd = open(filename, 'rb')

url = 'http://128.138.202.117:8080/scan'
headers = {'Authorization': 'my-api-key'}
image_metadata = {'key1': 'value1', 'key2': 'value2'}
data = {'name': 'image.jpg' }
files = {'file': fd}
r = requests.post(url, files=files, headers=headers, data=data)
print "r is ", r
print "response is ", r.json()
