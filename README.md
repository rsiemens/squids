# Development

## Getting Started

1. `poetry install`
2. Start up localstack `poetry run localstack start`
3. Create a test queue `aws --endpoint-url="http://localhost:4566" sqs create-queue --queue-name test-queue`
4. Start up the workers `poetry run python -m squids --queue test-queue --app tasks.app` (using the test `tasks.py` module included in the repo)
5. Start sending tasks:

```python
In [2]: long_running_email_task.send(to_addr='foo@domain.com', from_addr='bar@domain.com', body='Hello World!')
Running before send hook
Running after send hook
Out[2]:
{'MD5OfMessageBody': '4c65b04b14441852f8285b27b864d54e',
 'MessageId': '5c52797e-ce8c-ddd2-58e2-b7c9e0e4b026',
 'ResponseMetadata': {'RequestId': '3PXDRKUSE6U3G58DH7LN4R78VFBYALI9PWSEKPK8ATFOZAEVKEG3',
  'HTTPStatusCode': 200,
  'HTTPHeaders': {'content-type': 'text/html; charset=utf-8',
   'content-length': '322',
   'x-amzn-requestid': '3PXDRKUSE6U3G58DH7LN4R78VFBYALI9PWSEKPK8ATFOZAEVKEG3',
   'x-amz-crc32': '1145979214',
   'access-control-allow-origin': '*',
   'access-control-allow-methods': 'HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH',
   'access-control-allow-headers': 'authorization,cache-control,content-length,content-md5,content-type,etag,location,x-amz-acl,x-amz-content-sha256,x-amz-date,x-amz-request-id,x-amz-security-token,x-amz-tagging,x-amz-target,x-amz-user-agent,x-amz-version-id,x-amzn-requestid,x-localstack-target,amz-sdk-invocation-id,amz-sdk-request',
   'access-control-expose-headers': 'etag,x-amz-version-id',
   'connection': 'close',
   'date': 'Tue, 26 Apr 2022 03:43:54 GMT',
   'server': 'hypercorn-h11'},
  'RetryAttempts': 0}}
```


# Usage

See `tasks.py` for an example of using SQuidS.
