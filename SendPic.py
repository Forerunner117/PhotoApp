import sys,os
import pickle
import PIL
import pika
import md5

hostname= os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'

if len(sys.argv) < 2:
    print "Error! No image file specified!"
    print "Usage: %s <filename>" % sys.argv[0]
    sys.exit(1)

filename = sys.argv[1]
fd = open(filename, 'rb')
fileContents = fd.read()
#
# Prepare a tuple of the file name and file contents
#
tup = (filename,
       hashlib.md5(fileContents).hexdigest(),
       fileContents)
pickled = pickle.dumps(tup)
#
# You can print it out, but it is very long
print "pickled item is ", len(pickled),"bytes"

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=hostname))
channel = connection.channel()

channel.exchange_declare(exchange='logs',
                         type='fanout')

channel.basic_publish(exchange='scanners',
                      routing_key='',
                      body=pickled)
print " [x] Sent photo ", filename
connection.close()
