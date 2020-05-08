###### Twitter

Make sure you have a Twitter app created at:

That's where you'll get your:

CONSUMER_KEY

CONSUMER_SECRET

ACCESS_KEY

ACCESS_SECRET

If you have issues installing pyspark, you may have to install first.


refer to:
https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter
for details

Standard streaming API request parameters
https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters

locations ... for example '-74,40,-73,41' is NYC (bounding box)

on Windows:

execute:
```shell script
venv\Scripts\python.exe twitter\apache-spark-streaming-app.py
 ```
and then execute:
```shell script
venv\Scripts\python.exe twitter\twitter-http-client.py
```

