# Development

## Getting Started

1. `poetry install`
2. Start up localstack `poetry run localstack start`
3. Create a test queue `aws --endpoint-url="http://localhost:4566" sqs create-queue --queue-name test-queue`
4. Start up the workers `poetry run python -m squids --queue test-queue --app tasks.app` (using the test `tasks.py` module included in the repo)
5. Start sending tasks:

```python
In [1]: from tasks import printer, long_running_email_task

In [2]: long_running_email_task.send(to_addr='foo@domain.com', from_addr='bar@domain.com', body='Hello World!')
Sending {"task": "long_running_email_task", "args": ["foo@domain.com", "bar@domain.com", "Hello World!"], "kwargs": {}, "task_id": "40c2436c-7e4f-4627-8a04-08128d8e80a4"} to test-queue
Got response {'MD5OfMessageBody': '2f9077b514b1f459bf6a492abaed0945', 'MessageId': '78ad34db-abae-ec91-c236-b3d5db2e3627', 'ResponseMetadata': {'RequestId': '6ADEAZLN4EFY0UBW5JOR2KA0RI7VWJ0D9CN4O5H6R7E1Z33NADHM', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'text/html; charset=utf-8', 'content-length': '322', 'x-amzn-requestid': '6ADEAZLN4EFY0UBW5JOR2KA0RI7VWJ0D9CN4O5H6R7E1Z33NADHM', 'x-amz-crc32': '3277104638', 'access-control-allow-origin': '*', 'access-control-allow-methods': 'HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH', 'access-control-allow-headers': 'authorization,cache-control,content-length,content-md5,content-type,etag,location,x-amz-acl,x-amz-content-sha256,x-amz-date,x-amz-request-id,x-amz-security-token,x-amz-tagging,x-amz-target,x-amz-user-agent,x-amz-version-id,x-amzn-requestid,x-localstack-target,amz-sdk-invocation-id,amz-sdk-request', 'access-control-expose-headers': 'etag,x-amz-version-id', 'connection': 'close', 'date': 'Mon, 25 Apr 2022 17:45:23 GMT', 'server': 'hypercorn-h11'}, 'RetryAttempts': 0}}
Out[2]: '40c2436c-7e4f-4627-8a04-08128d8e80a4'
```


# Usage

See `tasks.py` for an example of using SQuidS.
