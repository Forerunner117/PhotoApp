import os
from flask import Flask, request, redirect, url_for
from werkzeug import secure_filename
import pickle
import PIL
import pika
import hashlib
import redis

hostname= os.environ['RABBIT_HOST'] \
          if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'
connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
channel = connection.channel()


app = Flask(__name__)

UPLOAD_FOLDER = '/tmp'
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

redisByChecksum = redis.Redis(host='redis-server.local', db=1)
redisByName = redis.Redis(host='redis-server.local', db=2)
redisMD5ByLicense = redis.Redis(host='redis-server.local', db=3)
redisNameByLicense = redis.Redis(host='redis-server.local', db=4)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/scan", methods=['POST', 'GET'])
def scan():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            sFileName =  os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save( sFileName )
            print "File is ", sFileName
            fd = open(sFileName, 'rb')
            fileContents = fd.read()
            #
            # Prepare a tuple of the file name and file contents
            #
            digest = hashlib.md5(fileContents).hexdigest()
            tup = (sFileName, digest, fileContents)
            pickled = pickle.dumps(tup)
            #
            # You can print it out, but it is very long
            #
            print "pickled item is ", len(pickled),"bytes"
            #
            # Send the picture to the scanners
            #
            channel.exchange_declare(exchange='scanners',type='fanout')
            channel.basic_publish(exchange='scanners',
                                  routing_key='',
                                  body=pickled)
            print " [x] Sent photo ", sFileName
            os.remove(sFileName)
            return '{"digest":"%s"}' % (digest)
        else:
            abort(403)

@app.route("/licenses-by-md5/<checksum>")
def licenses_by_md5(checksum):
    itemCount = redisByChecksum.llen(checksum)
    if itemCount > 0:
        val = [redisByChecksum.lindex(checksum, index) for index in range(itemCount)]
        valList = ",".join(val)
        return "Results : { '%s' }" % valList
    else:
        return "No Results"

@app.route("/licenses-by-name/<filename>")
def licenses_by_name(filename):
    itemCount = redisByName.llen(filename)
    if itemCount > 0:
        val = [redisByName.lindex(filename, index) for index in range(itemCount)]
        valList = ",".join(val)
        return "Results : { '%s' }" % valList
    else:
        return "No Results"

@app.route("/name-by-license/<license>")
def name_by_license(license):
    itemCount = redisNameByLicense.llen(license)
    if itemCount > 0:
        val = [redisNameByLicense.lindex(license, index) for index in range(itemCount)]
        valList = ",".join(val)
        return "Results : { '%s' }" % valList
    else:
        return "No Results"

@app.route("/md5-by-license/<license>")
def md5_by_license(license):
    itemCount = redisMD5ByLicense.llen(license)
    if itemCount > 0:
        val = [redisMD5ByLicense.lindex(license, index) for index in range(itemCount)]
        valList = ",".join(val)
        return "Results : { '%s' }" % valList
    else:
        return "No Results"


if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=8080)
