import sys,os
import pickle
import ScanPlate
import GetLatLon
import tempfile
import PIL
import pika

def imageType(filename):
    try:
        i=PIL.Image.open(filename)
        return i.format
    except IOError:
        return False

hostname= os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'

def photoInfo(pickled):
    #
    # You can print it out, but it is very long
    print "pickled item is ", len(pickled),"bytes"
    unpickled = pickle.loads(pickled)
    print "File name was", unpickled[0], "digest is ", unpickled[1]
    photoFile,photoName = tempfile.mkstemp("photo")
    os.write(photoFile, unpickled[2])
    os.close(photoFile)
    newPhotoName = photoName + '.' + imageType(photoName)
    os.rename(photoName, newPhotoName)
    print "Wrote it to ", newPhotoName
    print "License:", ScanPlate.getLikelyLicense( newPhotoName )
    print "GeoTag:", GetLatLon.getLatLon( newPhotoName )
    os.remove(newPhotoName)

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=hostname))
channel = connection.channel()

channel.exchange_declare(exchange='scanners',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='scanners',queue=queue_name)

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    photoInfo(body)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
