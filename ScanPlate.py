import sys
import subprocess
import json

def getLikelyLicense(filename, threshold = 85.0, debug=False):
    try:
        proc = subprocess.Popen(['/usr/bin/alpr',
                                 '-j',
                                 filename],
                                stdout=subprocess.PIPE)
        out,err = proc.communicate()
        results = json.loads(out)['results'][0]
        lyst = []
        if debug:
            print "results is ", len(results)
            print json.dumps(results, sort_keys=True,
                             indent=4, separators=(',', ': '))
        if 'candidates' in results:
            candidates = results['candidates']
            for key in candidates:
                if ('confidence' in key) and (key['confidence'] >= threshold):
                    if debug:
                        print "Match at ", key['plate'], "with ", key['confidence']
                    lyst.append((key['plate'], key['confidence']))
        return lyst
    except:
        return []

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Error! No image file specified!"
        print "Usage: %s <filename>" % sys.argv[0]
        sys.exit(1)
 
    plates = getLikelyLicense(sys.argv[1])
    print plates

