import json
import os
import sys
from datetime import datetime
from dateutil import parser
from Hashtag_Count import *
from LRU_Edge_Cache_Graph import *
from Adjacent_Set_Cache_Graph import *

# Import the necessary methods from tweepy library
import tweepy
from tweepy.stream import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class AvgDegreeGenerator:
  """ Generate average degree, with realtime tweets.
  As tweets are realtime, I would open ft1 and output, write data and close them, 
  whenever I receive one tweet. It will write data to disk in a non-sequential manner. 
  As the experiment data in average_degree_from_local_tweet.py is about 9 seconds parsing 18729 tweets,
  the speed is fast enough. Then I could open and close ft1 and output file frequently 
  to let users observe the realtime data.
  """

  def __init__(self, raw_tweet_file_name, ft1_file_name, output_file_name):
    self.raw_tweet_file_name = raw_tweet_file_name
    self.ft1_file_name = ft1_file_name
    self.output_file_name = output_file_name
    self.tweet_parser = TweetParser()
    self.edge_cache = GraphLRUEdgeCache()
    self.adj_graph_cache = GraphAdjacentSetCache()
    self.unicode_tweet_num = 0
    self.start_time = None
    self.is_start = False

    # open and clear the context
    ft1 = open(self.ft1_file_name, 'w')
    output = open(self.output_file_name, 'w')
    ft1.close()
    output.close()

  # Input raw_tweet, parse it and write it to ft1.
  # When timestamp of current tweet is 60 seconds older than beginning tweet, write average degree to output
  def parse_tweet_and_generate_degree(self, line):
    parsed_tweet, cur_time = self.parse_one_tweet(line)
    if None != parsed_tweet: 
      ft1 = open(self.ft1_file_name, 'ab')
      ft1.write(parsed_tweet + '\n')
      ft1.close()
      if False == self.is_start:
        self.start_time = cur_time
        self.is_start = True
      elif (cur_time - timedelta(seconds = 60) > self.start_time):
        output = open(self.output_file_name, 'ab')
        if 0 == len(self.adj_graph_cache.cache):
          output.write(str(0) + '\n')
        else:
          output.write(format(2.0 * len(self.edge_cache.cache) / len(self.adj_graph_cache.cache), '.2f') + '\n')
        output.close()

  # Input raw_tweet, parse it. 
  # Store edges to self.edge_cache, store nodes and edges to self.adj_graph_cache
  # return parsed tweet and datetime, if there's no text in the tweet, return None, None
  def parse_one_tweet(self, line):
    tweet_json = json.loads(line)
    text, time, has_unicode = self.tweet_parser.parse_raw_tweet(tweet_json)
    if None != text:
      if has_unicode: self.unicode_tweet_num += 1
      hashtag_pair_list = self.tweet_parser.get_hashtag_pair_list(text)
      time_format = self.tweet_parser.get_datetime(time)
      remove_list = self.edge_cache.set_hashtag_pair_list(hashtag_pair_list, time_format)
      self.adj_graph_cache.set_hashtag_pair_list(hashtag_pair_list)
      self.adj_graph_cache.remove_hashtag_pair(remove_list)
      return (text + ' (timestamp: ' + time + ')'), time_format
    return None, None

  # When keyboard Interrupt, write down numbers of tweets contained unicode
  def handle_keyboard_interrupt(self):
    ft1 = open(self.ft1_file_name, 'ab')
    ft1.write('\n' + str(self.unicode_tweet_num) + ' tweets contained unicode.')
    ft1.close()

class StdOutListener(StreamListener):
  """ A listener handles tweets that are the received from the stream.
  And write parsed tweets to ft1, write average degree to output.
  """
  def __init__(self, filename, generator):
    self.filename = filename
    self.generator = generator
    f = open(self.filename, 'w')
    f.close()

  # this is the event handler for new data
  def on_data(self, data):
    if not os.path.isfile(self.filename):    # check if file doesn't exist
      f = file(self.filename, 'w')
      f.close()
    with open(self.filename, 'ab') as f:
      # print "writing to {}".format(self.filename)
      f.write(data)
      self.generator.parse_tweet_and_generate_degree(data)
    f.closed
      
  # this is the event handler for errors    
  def on_error(self, status):
    print(status)

if __name__ == '__main__':
  # authentication from the credentials file above
  twitter_file = open(sys.argv[1])  
  twitter_cred = json.load(twitter_file)
  twitter_file.close()
  auth = OAuthHandler(twitter_cred["consumer_key"], twitter_cred["consumer_secret"])
  auth.set_access_token(twitter_cred["access_token"], twitter_cred["access_token_secret"])

  generator = AvgDegreeGenerator(sys.argv[2], sys.argv[3], sys.argv[4])
  listener = StdOutListener(sys.argv[2], generator)

  print "Use CTRL + C to exit at any time.\n"
  stream = Stream(auth, listener)
  try:
    stream.filter(locations=[-180,-90,180,90]) # this is the entire world, any tweet with geo-location enabled
  except KeyboardInterrupt:
    listener.generator.handle_keyboard_interrupt()
    raise