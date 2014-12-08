##
## Sends photos to the web server specified on
## the command line. Wants for input before sending a given
## photo. Downloads the photo database from and
## selects random photo from that.
##
import sys,os
import requests
import json
import getopt
import urllib
import gdbm
import random
import traceback
import pprint

try:
    opts, args = getopt.getopt(sys.argv[1:],
                               "hd:s:v",
                               ["help", "db=", "server="])
except getopt.GetoptError as err:
    # print help information and exit:
    print str(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

output = None
verbose = False
dbUrl = "http://systems.cs.colorado.edu/~grunwald/geotagged-small.gdbm"
serverName = None

for o, a in opts:
    if o == "-v":
        verbose = True
    elif o in ("-h", "--help"):
        print "%s: [--db %s] [--server %s]" % (sys.argv[0], dbname, serverName)
        usage()
        sys.exit()
    elif o in ("-d", "--db"):
        dbUrl = a
    elif o in ("-s", "--server"):
        serverName = a
    else:
        assert False, "unhandled option"

if serverName == None:
    print "You must specify a server name"
    print "Usage: %s --server hostname" % ( sys.argv[0] )
    print "or %s -s hostname" % ( sys.argv[0] )
    sys.exit(1)

dbFileName = dbUrl.split('/')[-1]
print "dbUrl is", dbUrl, "dbFileName is", dbFileName
print "serverName is", serverName

if os.path.isfile(dbFileName):
    print "Database file", dbFileName, "exists - not retrieving"
else:
    #
    # Download the file and give status indicator
    #
    u = urllib.urlopen(dbUrl)
    f = open(dbFileName, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (dbFileName, file_size)

    file_size_dl = 0
    block_sz = 8192
    frac = 0.0
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        f.write(buffer)
        part = float(file_size_dl)/file_size
        if part >= frac + 0.1:
            frac = part
            print "..", int(frac*100), "%.."
    f.close()

print "Counting number of pictur URL's..."

db = gdbm.open(dbFileName, 'r')
numUrls = 0
for key in db.keys():
    numUrls = numUrls + 1
print "Database contains", numUrls, "total picture URL's"



while True:
    picNum = random.randrange(0, numUrls)
    codedUrl = db[str(picNum)]
    server,url = codedUrl.split(':')
    flickrUrl = "http://farm%s.staticflickr.com%s" % ( server, url )
    print "Url #", picNum, "is", flickrUrl

    tmpfile, headers = urllib.urlretrieve(flickrUrl)
    try:
        print "tmpfile is ", tmpfile
        fd = open(tmpfile, 'rb')
        files = {'file': (flickrUrl, fd)}
        serverUrl = serverName + "/scan"
        print "making request.."
        response = requests.post(serverUrl, files=files)
        print "Reponse:", response
        if response == requests.codes.ok:
            print pprint.pprint(response.json())
    except:
        print "Exception in scanning file"
        traceback.print_exc(file=sys.stdout)
    if os.path.isfile(tmpfile):
        os.remove(tmpfile)
    raw_input('Hit return to download another..')

