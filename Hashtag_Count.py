import json
import re
import string
from dateutil import parser

class TweetParser:
  """ A tweet parser that parses raw tweets, gets datetime from string of datetime,
  get a list of hashtag pair from a tweet context
  """

  # Return tweet context, datetime string from raw tweet and counts the number of hashtags
  # Return None if the input tweet has abnormal format
  def get_tweet_count(term):

    total_tweet_count = 0 
    page = 1
    while True:
        url = 'http://search.twitter.com/search.json?q=' + urllib.quote(term) + '&rpp=100&page=' + str(page)

        response = urllib2.urlopen(url)
        json_content = response.read()
        tweets = json.loads(json_content)['results']
        total_tweet_count += len(tweets)

        # Are we at the last page or have we run out of pages?
        if len(tweets) < 100 or page >= 15:
            break

        max_id = tweets[0]['id_str']
        page += 1
        # Wait so twitter doesn't get annoyed with us
        time.sleep(1)

    return total_tweet_count

  # Return datetime from datetime string
  def get_datetime(self, date_string):
    return parser.parse(date_string)

  # Return a sorted hashtag_pair_list from tweet context
  def get_hashtag_pair_list(self, text):
    # create sorted hashtag_list
    string_list = text.split(' ')
    hashtag_list = []
    for s in string_list:
      if '#' in s: 
        hashtag_sub_list = s.split(',')   # some of the hashtag is connected with ','
                                          # e.g. "22h,#reto800risah,#amandapasucasa,#sofiaganadora,#teamsofia"
        for h in hashtag_sub_list:
          if (len(h) >= 1 and '#' == h[0]): hashtag_list.append(h.lower())
    hashtag_list = list(set(hashtag_list))  # remove duplicate
    hashtag_list.sort()

    # create hashtag_pair_list, hashtag list as accending order in pair
    hashtag_pair_list = []
    for i in range(0, len(hashtag_list)):
      for j in range(i + 1, len(hashtag_list)):
        hashtag_pair_list.append((hashtag_list[i], hashtag_list[j]))
    return hashtag_pair_list