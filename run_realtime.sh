#!/usr/bin/env bash

# you should have twitter credential as following format and input the directory as the second argument
# the default tweet credential file path is ./data-gen/.twitter

# {
# "access_token":"your access_token",
# "access_token_secret":"your access_token_secret",
# "consumer_key":"your consumer_key",
# "consumer_secret":"your consumer_secret"
# }

# argv[0]: script that process realtime tweets
# argv[1]: directory of twitter credential
# argv[2]: directory of tweet_input
# argv[3]: directory of feature 1's result
# argv[4]: directory of feature 2's result
python ./src/average_degree_from_realtime.py ./data-gen/.twitter ./tweet_input/tweets.txt ./tweet_output/ft1.txt ./tweet_output/ft2.txt