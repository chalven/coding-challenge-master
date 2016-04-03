import json
import os
import sys
from datetime import datetime
from dateutil import parser
from Hashtag_Count import *
from LRU_Edge_Cache_Graph import *
from Adjacent_Set_Cache_Graph import *

class AvgDegreeGenerator:
  """ Generate average degree, with information from the already created tweet file.
  As tweets are pre created, I write parsed tweets into ft1 when I've parsed 3000 tweets. 
  In this case, I can write data sequentially. 

  The experiment data is about 8 seconds to parse 18729 tweets.
  """

  def __init__(self, raw_tweet_file_name, ft1_file_name, output_file_name):
    self.raw_tweet_file_name = raw_tweet_file_name
    self.ft1_file_name = ft1_file_name
    self.output_file_name = output_file_name
    self.tweet_parser = TweetParser()
    self.edge_cache = GraphLRUEdgeCache()
    self.adj_graph_cache = GraphAdjacentSetCache()
    self.unicode_tweet_num = 0

  # Parse tweet from pre_created tweet file. 
  # Write parsed tweets to ft1 after processing 3000 tweets.
  # When timestamp of current tweet is 60 seconds older than beginning tweet, write average degree to output
  def parse_tweet_and_generate_degree(self):
    ft1 = open(self.ft1_file_name, 'w')
    output = open(self.output_file_name, 'w')
    is_start = False
    start_time = None
    count = 0
    parsed_tweet_list = []

    with open(self.raw_tweet_file_name) as raw_tweet_file:
      for line in raw_tweet_file:
        count += 1
        parsed_tweet, cur_time = self.parse_one_tweet(line)
        if None != parsed_tweet: 
          parsed_tweet_list.append(parsed_tweet)
          if False == is_start:
            start_time = cur_time
            is_start = True
          elif (cur_time - timedelta(seconds = 60) > start_time):
            if 0 == len(self.adj_graph_cache.cache):
              output.write(str(0) + '\n')
            else:
              output.write(format(2.0 * len(self.edge_cache.cache) / len(self.adj_graph_cache.cache), '.2f') + '\n')

          # write to ft1 when parse 3000 tweets
          if len(parsed_tweet_list) > 3000:
            ft1.write('\n'.join(parsed_tweet_list))
            del parsed_tweet_list[:]
    raw_tweet_file.close()
    if len(parsed_tweet_list) > 0: ft1.write('\n'.join(parsed_tweet_list))
    ft1.write('\n\n' + str(self.unicode_tweet_num) + ' tweets contained unicode.')
    ft1.close()
    output.close()
    return count

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

if __name__ == '__main__':
  generator = AvgDegreeGenerator(sys.argv[1], sys.argv[2], sys.argv[3])
  start_time = datetime.now()
  count = generator.parse_tweet_and_generate_degree()
  print str(count) + ' tweets are processed.'
  print 'total_time: ' + str(datetime.now() - start_time)