import sys,os
import pickle
import ScanPlate
import GetLatLon
import tempfile
import PIL
import pika
import redis

redisByChecksum = redis.Redis(host='redis-server.local', db=1)
redisByName = redis.Redis(host='redis-server.local', db=2)
redisMD5ByLicense = redis.Redis(host='redis-server.local', db=3)
redisNameByLicense = redis.Redis(host='redis-server.local', db=4)

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
    oldName = unpickled[0]
    digest = unpickled[1]
    print "File name was", oldName, "digest is ", digest
    photoFile,photoName = tempfile.mkstemp("photo")
    os.write(photoFile, unpickled[2])
    os.close(photoFile)
    newPhotoName = photoName + '.' + imageType(photoName)
    os.rename(photoName, newPhotoName)
    print "Wrote it to ", newPhotoName
    licenses = ScanPlate.getLikelyLicense( newPhotoName )
    print "License:", licenses
    geotags = GetLatLon.getLatLon( newPhotoName )
    print "GeoTag:", geotags

    if (len(licenses) > 0):
	redisAdd(licenses, geotags, oldName, digest)

    os.remove(newPhotoName)    

def redisAdd(plate, geotag, photoName, md5):
    # Found a license plate at this point.
    # Remove preceeding '/tmp/'
    cleanName = re.sub('\/tmp\/', '', photoName)
    #
    ## Consider locking the db
    #
    if redisByChecksum.llen(md5) == 0:
        print 'Inserting', md5, 'into redisByChecksum and redisMD5ByLicense'
        for p in plate:
            redisByChecksum.lpush(md5, p[0])
            redisMD5ByLicense.lpush(p[0], md5)

    if redisByName.llen(cleanName) == 0:
        print 'Inserting', cleanName, 'into redisByName and redisNameByLicense'
        for p in plate:
            redisByName.lpush(cleanName, p[0])
            redisNameByLicense.lpush(p[0], cleanName)

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
