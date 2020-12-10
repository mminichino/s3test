Lightweight S3 test utility.

Run PUT test with a total of 64 1MiB objects with 32 threads:
````
$ ./s3test.py -e https://s3.company.com -p awsprofile -b bucket -s 1048576 -c 64 -f prefix -o put -t 32 -v
````

Run GET test with 64 objects with the given prefix (prefix-1 ... prefix-64, objects must already exist):
````angular2html
$ ./s3test.py -e https://s3.company.com -p awsprofile -b bucket -c 64 -f prefix -o get -d /home/user/test -v
````

Determine best thread count for a test scenario:
````
./s3test.py -e https://s3.company.com -p awsprofile -b bucket -s 1048576 -c 64 -f prefix -o put -v -m
````