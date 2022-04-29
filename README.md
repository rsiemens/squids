# Getting Started

1. Create a queue `aws --endpoint-url="http://localhost:4566" sqs create-queue --queue-name test`
2. Start sending tasks and consuming them:

```python
In [1]: from tasks import app, printer

In [2]: printer.send('I traveled through an SQS queue!')
Running before send hook
Running after send hook
Out[2]:
{'MD5OfMessageBody': 'e8b241481ecc05698a5003f22339baa7',
 'MessageId': '0d7926bd-abe0-0d13-f989-ca2d6afb340e',
 'ResponseMetadata': {'RequestId': '4ZM803EGQEZJ259XMLEG9H1EYSBDU1X3K1ZEPMP000CF05U8SS2T',
  'HTTPStatusCode': 200,
  'HTTPHeaders': {'content-type': 'text/html; charset=utf-8',
   'content-length': '322',
   'x-amzn-requestid': '4ZM803EGQEZJ259XMLEG9H1EYSBDU1X3K1ZEPMP000CF05U8SS2T',
   'x-amz-crc32': '1312475348',
   'access-control-allow-origin': '*',
   'access-control-allow-methods': 'HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH',
   'access-control-allow-headers': 'authorization,cache-control,content-length,content-md5,content-type,etag,location,x-amz-acl,x-amz-content-sha256,x-amz-date,x-amz-request-id,x-amz-security-token,x-amz-tagging,x-amz-target,x-amz-user-agent,x-amz-version-id,x-amzn-requestid,x-localstack-target,amz-sdk-invocation-id,amz-sdk-request',
   'access-control-expose-headers': 'etag,x-amz-version-id',
   'connection': 'close',
   'date': 'Thu, 28 Apr 2022 05:13:57 GMT',
   'server': 'hypercorn-h11'},
  'RetryAttempts': 0}}

In [3]: consumer = app.create_consumer('test')

In [4]: consumer.consume()
Running before task hook
I traveled through an SQS queue!
Running after task hook
```

Or you can use the squids CLI workers to continuously consume from a queue for you!

```
$ python -m squids --queue test --app tasks.app
                                    .%%%:
                                  -%%%:#%%=
          .%%%:                  %%-%.::%+#%                  .%%%:
        -%%%:#%%=              =%::% ::::% :%*              -%%%:#%%=
       %%-%.::%+#%            #%.:%=:::::-% :%%            %%-%.::%+#%
     =%::% ::::% :%*         %%::-% ::::::%-::#%         =%::% ::::% :%*
    #%.:%=:::::-% :%%       %#:::%.::::::::% :-+%       #%.:%=:::::-% :%%
   %%::-% ::::::%-::#%     #%.:::# ::::::::#-:::##     %%::-% ::::::%-::#%
  %#:::%.::::::::% :-+%    % :::%.::::::::::% :::%    %#:::%.::::::::% :-+%
 #%.:::# ::::::::#-:::##  ++ .::% ::-:::-:::% :. :#  #%.:::# ::::::::#-:::##
 % :::%.::::::::::% :::%   %#%+*#::%#%:%%%::=%+%%%   % :::%.::::::::::% :::%
++ .::% ::-:::-:::% :. :#     -% ::%.#-%.%:::#=     ++ .::% ::-:::-:::% :. :#
 %#%+*#::%#%:%%%::=%+%%%       % ::##%:%%%:::%       %#%+*#::%#%:%%%::=%+%%%
    -% ::%.#-%.%:::#=          #. .:::::::::.%          -% ::%.#-%.%:::#=
     % ::##%:%%%:::%            #%*-.   .:*%%            % ::##%:%%%:::%
     #. .:::::::::.%             .-#%%%%%#-              #. .:::::::::.%
      #%*-.   .:*%%                                       #%*-.   .:*%%
       .-#%%%%%#-              :%%#  %##  ##%=             .-#%%%%%#-
                               % :% #+:-% %::%
     :%%#  %##  ##%=           % :%:%:::% %.:#           :%%#  %##  ##%=
     % :% #+:-% %::%           %%#%  %#%. %%#%           % :% #+:-% %::%
     % :%:%:::% %.:#                                     % :%:%:::% %.:#
     %%#%  %#%. %%#%                                     %%#%  %#%. %%#%

            /######   /######            /##       /##  /######
           /##__  ## /##__  ##          |__/      | ## /##__  ##
          | ##  \__/| ##  \ ## /##   /## /##  /#######| ##  \__/
          |  ###### | ##  | ##| ##  | ##| ## /##__  ##|  ######
           \____  ##| ##  | ##| ##  | ##| ##| ##  | ## \____  ##
           /##  \ ##| ##/## ##| ##  | ##| ##| ##  | ## /##  \ ##
          |  ######/|  ######/|  ######/| ##|  #######|  ######/
           \______/  \____ ### \______/ |__/ \_______/ \______/
                          \__/

[config]
  app = test
  queue = test
  workers = 8
  report-interval = 300
  polling-wait-time = 5

[tasks]
  - tasks.printer
  - tasks.long_running_email_task

Running before task hook
I traveled through an SQS queue!
Running after task hook
```



# Usage

See `tasks.py` for an example of using SQuidS.