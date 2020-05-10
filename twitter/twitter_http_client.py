#!/bin/python3

import socket
import sys
import requests
import requests_oauthlib
import json
from twitter import twitter_config

# refer to:
# https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter
# for details
#
# Standard streaming API request parameters
# https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
#
# locations ... for example '-74,40,-73,41' is NYC (bounding box)
#
# on Windows:
# >venv\Scripts\python.exe twitter\twitter_http_client.py

# query params
MY_LOCATION = '-90,25,-60,50'
TRACK = '#'
LANGUAGE = 'en'

# socket connection
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None

my_auth = requests_oauthlib.OAuth1(twitter_config.CONSUMER_KEY, twitter_config.CONSUMER_SECRET, twitter_config.ACCESS_TOKEN, twitter_config.ACCESS_SECRET)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', LANGUAGE), ('locations', MY_LOCATION), ('track', TRACK)]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def read_tweets_to_socket(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print("------------------------------------------")
            tweet_data = bytes(tweet_text + "\n", 'utf-8')
            tcp_connection.send(tweet_data)
        except TypeError:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
        except json.decoder.JSONDecodeError:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


if __name__ == '__main__':
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP,TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    # def accept(self) -> Tuple[socket,Any]
    conn, addr = s.accept()
    print("Connected... streaming tweets: ",addr)
    resp = get_tweets()
    read_tweets_to_socket(resp,conn)
