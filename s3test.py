#!/usr/bin/env python
#
#

import getopt
import sys
import os
import datetime
import urllib3
import math
import string
import signal
import random
import tempfile
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError

def usage():
    print("Lightweight S3 Tester")
    print("Usage: " + sys.argv[0] + " [-v] [-b bucket] [-c object_count] [-e end_point] [-s data_size] [-p profile] [-f file_prefix] [-d dest_dir] [-o test_op]")

def signal_handler(signal,frame):

    print("")
    print("Interrupt caught, exiting.")
    sys.exit(0)

def formatSize(bytes):

    if bytes >= 1125899906842624:
        unit = "PiB"
        divisor = 1125899906842624
    elif bytes >= 1099511627776:
        unit = "TiB"
        divisor = 1099511627776
    elif bytes >= 1073741824:
        unit = "GiB"
        divisor = 1073741824
    elif bytes >= 1048576:
        unit = "MiB"
        divisor = 1048576
    elif bytes >= 1024:
        unit = "KiB"
        divisor = 1024
    else:
        unit = "bytes"
        divisor = 1

    quantity = bytes / divisor
    quantity = round(quantity, 1)
    print_str = str(quantity) + ' ' + str(unit)

    return print_str

class parse_args:

    def __init__(self):

        self.arglist = []
        self.bucketName = None
        self.awsProfile = None
        self.dataSize = 65536
        self.endPoint = None
        self.verboseFlag = False
        self.opCount = 1
        self.filePrefix = 'objdata'
        self.opType = 'put'
        self.destDir = '/dev/null'
        self.argCount = 0

    def parse(self):
        options, remainder = getopt.getopt(sys.argv[1:], 'o:f:hvc:b:p:s:e:d:', self.arglist)

        self.argCount = len(options)
        for opt, arg in options:
            if opt in ('-b', '--bucket'):
                self.bucketName = arg
            elif opt in ('-p', '--profile'):
                self.awsProfile = arg
            elif opt in ('-s', '--size'):
                try:
                    self.dataSize = int(arg)
                except ValueError as e:
                    print("Size must be a number.")
                    sys.exit(1)
            elif opt in ('-e', '--endpoint'):
                self.endPoint = arg
            elif opt in ('-v', '--verbose'):
                self.verboseFlag = True
            elif opt in ('-c', '--count'):
                try:
                    self.opCount = int(arg)
                except ValueError as e:
                    print("Count must be a number.")
                    sys.exit(1)
            elif opt in ('-f', '--fileprefix'):
                self.filePrefix = arg
            elif opt in ('-o', '--operation'):
                self.opType = arg
            elif opt in ('-d', '--dest'):
                if os.path.isdir(arg):
                    self.destDir = arg
                else:
                    print("Directory %s does not exist." % arg)
                    sys.exit(1)
            elif opt in ('-h', '--help'):
                usage()
                sys.exit(0)
            else:
                usage()
                sys.exit(1)

class tester:

    def __init__(self, args):

        self.token = args
        self.bucketName = self.token.bucketName
        self.verboseFlag = self.token.verboseFlag
        self.opType = self.token.opType
        self.opCount = self.token.opCount
        self.filePrefix = self.token.filePrefix
        self.dataSize = self.token.dataSize
        self.endPoint = self.token.endPoint
        self.awsProfile = self.token.awsProfile
        self.destDir = self.token.destDir
        self.percentage = 0
        self.xfer_total = 0
        self.current_file = 0
        self.last_file = 0
        self.start_time = datetime.datetime.now().replace(microsecond=0)
        self.end_time = datetime.datetime.now().replace(microsecond=0)
        config = Config(
            retries={
                'max_attempts': 5,
                'mode': 'standard'
            }
        )
        try:
            if self.awsProfile:
                self.s3session = boto3.Session(profile_name=self.awsProfile,)
            else:
                self.s3session = boto3.Session(profile_name="default",)
        except botocore.exceptions.ProfileNotFound as e:
            print("Error: %s" % str(e))
            sys.exit(1)
        self.s3 = self.s3session.client('s3', endpoint_url=self.endPoint, verify=False, config=config)

    def register_start(self):

        self.start_time = datetime.datetime.now().replace(microsecond=0)
        print("Start at %s" % self.start_time.strftime("%m/%d/%y %I:%M%p"))

    def register_end(self):

        self.end_time = datetime.datetime.now().replace(microsecond=0)
        print("End at %s" % self.end_time.strftime("%m/%d/%y %I:%M%p"))
        print("Run time: ", end='')
        print(self.end_time - self.start_time)

    def status_callback(self, number):

        if self.last_file == self.current_file:
            return

        self.percentage += (1 / self.opCount) * 100
        self.last_file = self.current_file

        print("File %d of %d in progress, %d%% completed ... " % (self.current_file, self.opCount, self.percentage), end='\r')

    def upload_file(self, file_name, bucket, name=None):

        if name is None:
            name = file_name

        try:
            response = self.s3.upload_file(file_name, bucket, name, Callback=self.status_callback)
            if self.current_file == self.opCount:
                print("")
        except (botocore.exceptions.ClientError, boto3.exceptions.S3UploadFailedError) as e:
            if self.percentage > 0:
                print("")
                print("Transferred %s" % formatSize(round(self.xfer_total * (self.percentage/100))))
            print("Can not upload object %s: %s" % (name, str(e)))
            sys.exit(1)

    def download_file(self, obj_name, bucket, dest=None):

        if dest is None:
            dest = '/dev/null'

        try:
            response = self.s3.download_file(bucket, obj_name, dest, Callback=self.status_callback)
            if self.current_file == self.opCount:
                print("")
        except (ClientError, PermissionError) as e:
            if self.percentage > 0:
                print("")
                print("Transferred %s" % formatSize(round(self.xfer_total * (self.percentage/100))))
            print("Can not download object %s: %s" % (obj_name, str(e)))
            sys.exit(1)

    def delete_file(self, obj_name, bucket):

        try:
            response = self.s3.delete_object(Bucket=bucket, Key=obj_name)
            self.status_callback(None)
            if self.current_file == self.opCount:
                print("")
        except (ClientError) as e:
            print("Can not delete object %s: %s" % (obj_name, str(e)))
            sys.exit(1)

    def list_bucket(self, quiet=False):

        if self.bucketName is None:
            print("Error: Bucket name is required.")
            sys.exit(1)

        bucket_name = self.bucketName
        obj_pattern = self.filePrefix + '.+'
        obj_pattern = '^' + obj_pattern + '$'

        try:
            bucket_region = self.s3.get_bucket_location(Bucket=bucket_name)
            self.bucketRegion = bucket_region
        except ClientError as e:
            print("Error: connection to %s failed: %s" % (self.endPoint, str(e)))
            sys.exit(1)

        try:
            kwargs = {'Bucket': bucket_name}
            while True:
                block = self.s3.list_objects_v2(**kwargs)
                for obj_entry in block['Contents']:
                    try:
                        obj_prefix, obj_number = str(obj_entry['Key']).split('-')
                        if int(obj_number) <= int(self.opCount) and obj_prefix == self.filePrefix:
                            self.xfer_total += obj_entry['Size']
                            if not quiet:
                                print("%s" % obj_entry['Key'])
                    except ValueError as e:
                        continue
                if block['IsTruncated']:
                    kwargs['ContinuationToken'] = block['NextContinuationToken']
                else:
                    break
        except (botocore.exceptions.ClientError, botocore.exceptions.EndpointConnectionError) as e:
            print("Error: can not connect to bucket %s: %s" % (bucket_name,str(e)))
            sys.exit(1)

    def random_string(self, width: int, pool: str = string.ascii_letters) -> set:

        buffer = ''
        while len(buffer) < width:
            byte = random.choices(pool, k=1)
            buffer += str(byte[0])

        return buffer

    def create_test_file(self, size):

        loop_count = math.trunc(size / 1024)
        loop_remainder = size % 1024

        temp_fd, filename = tempfile.mkstemp()
        try:
            with open(temp_fd, 'w') as test_file:
                if loop_count > 0:
                    data = self.random_string(width=1024)
                    for x in range(loop_count):
                        test_file.write(data)
                if loop_remainder > 0:
                    data = self.random_string(width=loop_remainder)
                    test_file.write(data)
        except OSError as e:
            print("Can not write temp file: %s" % str(e))
            sys.exit(1)

        test_file.close()
        return filename

    def put_test(self):

        size = self.dataSize
        count = self.opCount
        self.xfer_total = count * size

        if self.bucketName is None:
            print("Error: Bucket name is required.")
            sys.exit(1)

        if self.verboseFlag:
            print("Beginning PUT test with %d objects of size %s" % (count, formatSize(size)))
            print("Total size: %s" % formatSize(self.xfer_total))

        test_file = self.create_test_file(size)

        for x in range(count):
            self.current_file = x + 1
            obj_name = self.filePrefix + '-' + str(self.current_file)
            self.upload_file(test_file, self.bucketName, name=obj_name)

        os.unlink(test_file)

    def get_test(self):

        count = self.opCount
        if self.bucketName is None:
            print("Error: Bucket name is required.")
            sys.exit(1)

        self.list_bucket(quiet=True)

        if self.verboseFlag:
            print("Beginning GET test for %d objects" % count)
            print("Total size: %s" % formatSize(self.xfer_total))

        for x in range(count):
            self.current_file = x + 1
            obj_name = self.filePrefix + '-' + str(self.current_file)
            if self.destDir == '/dev/null':
                dest_name = '/dev/null'
            else:
                dest_name = self.destDir + '/' + obj_name
            self.download_file(obj_name, self.bucketName, dest=dest_name)

    def delete_test(self):

        count = self.opCount
        if self.bucketName is None:
            print("Error: Bucket name is required.")
            sys.exit(1)

        if self.verboseFlag:
            print("Beginning DELETE test for %d objects" % count)

        for x in range(count):
            self.current_file = x + 1
            obj_name = self.filePrefix + '-' + str(self.current_file)
            self.delete_file(obj_name, self.bucketName)

def main():

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    signal.signal(signal.SIGINT, signal_handler)

    runargs = parse_args()
    runargs.parse()

    test = tester(runargs)

    if runargs.verboseFlag:
        test.register_start()

    if test.opType == 'put':
        test.put_test()
    elif test.opType == 'list':
        test.list_bucket()
    elif test.opType == 'get':
        test.get_test()
    elif test.opType == 'delete':
        test.delete_test()
    else:
        print("Operation %s not implemented." % test.opType)
        sys.exit(1)

    if runargs.verboseFlag:
        test.register_end()

if __name__ == '__main__':

    try:
        main()
    except SystemExit as e:
        if e.code == 0:
            os._exit(0)
        else:
            os._exit(e.code)