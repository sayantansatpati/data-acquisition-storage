__author__ = 'ssatpati'

import object_storage
import time
import multiprocessing

USER = "SLOS530867-3:SL530867"
API_KEY = "46a4ba15b984f2bda6ec9132a9b6205d5c618bdb0cc18e74c121ad0e9f2ca059"
container = "container2"

f = "/Users/ssatpati/0-DATASCIENCE/sem-3/scaling/assignments/swift/ngram-1GB.csv"
f_name = "ngram-1GB.csv"

sl_storage = object_storage.get_client(USER, API_KEY, datacenter='dal05')


def upload_download():
    print(sl_storage.containers())

    # Delete container (if present)
    try:
        sl_storage[container].delete_all_objects()
        sl_storage[container].delete(True)
        print("Container Deleted")
    except Exception as e:
        print e

    # Create
    sl_storage[container].create()
    print("Container Created")
    print(sl_storage.containers())

    print("~~~~ Upload/Download in Sequence ~~~~")
    # Upload/Download Files Twice
    for i in xrange(2):
        file_name = f_name + "-" + str(i)
        upload(file_name)

        print(sl_storage[container].objects())

        download(file_name)

    print("~~~~ Upload/Download in Parallel ~~~~")
    file_name = f_name + "-p"
    p1 = multiprocessing.Process(target=upload, args=(file_name,))
    p2 = multiprocessing.Process(target=download, args=(file_name,))

    p1.start()
    p2.start()

    p1.join()
    p2.join()


def upload(file_name):
    print "Uploading File: ", file_name
    s = time.time()
    sl_storage[container][file_name].create()
    sl_storage[container][file_name].load_from_filename(f)
    e = time.time()
    print("### File Uploaded, Time Taken(s): {0}".format(e-s))
    print("### Upload Transfer Rate (KB/sec): {0}".format(1024*1024*1.0/(e-s)))


def download(file_name):
    s = time.time()
    print "Downloading File: ", file_name
    sl_storage[container][file_name].save_to_filename(file_name)
    e = time.time()
    print("### File Download, Time Taken(s): {0}".format(e-s))
    print("### Download Transfer Rate (KB/sec): {0}".format(1024*1024*1.0/(e-s)))

if __name__ == '__main__':
    upload_download()