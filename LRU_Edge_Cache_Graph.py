from datetime import *

class GraphLRUEdgeCache:
  """ Store graph edges with LRU (least recently used) algorithm.
  Implement LRU algorithm with HashMap and Double LinkedList. 
  The key of the HashMap is a hashtag_pair (e.g. ('#apache', '#hadoop')), 
  while the value is a doubly LinkedList Node which contains the same hashtag_pair, 
  the datetime of the tweet and the two linked list pointers, Next and Previous.
  The Double LinkedList is sorted in ascending order with datetime, 
  which would be helpful to remove timeout hashtag_pair..

  This data structure aims to calculate the average degree with tweets in latest 60s 
  and remove the timeout tweets. It also provides the edge number of every node.
  """

  class Node:
    """ Double LinkedList Node
    """
    def __init__(self, key, val):
      self.key = key  # hashtage_pair
      self.val = val  # datetime
      self.prev = None
      self.next = None

  def __init__(self):
    self.cache = {}   # hashtag_pair map to related Node
    self.head = self.Node(None, None)
    self.tail = self.Node(None, None)
    self.head.next = self.tail
    self.tail.next = self.head

  # Get related Node in the DLinkedList, keep the latest access Node in the tail of DLinkedList
  def get(self, key):
    if key not in self.cache:
      return None
    else:
      current = self.cache[key]
      current.prev.next = current.next
      current.next.prev = current.prev
      self.move_to_tail(current)
      return current.val

  # Set <key, value> pair to the HashMap, and keep the latest access Node in the tail of DLinkedList
  def set(self, key, value):
    if None != self.get(key):
      self.cache.get(key).val = value
    else:
      current = self.Node(key, value)
      if 0 == len(self.cache):
        self.head.next = current
        current.prev = self.head
        current.next = self.tail
        self.tail.prev = current
      else:
        self.move_to_tail(current)
      self.cache[key] = current

  # Move the current Node to tail of the DLinkedList
  def move_to_tail(self, current):
    self.tail.prev.next = current
    current.prev = self.tail.prev
    current.next = self.tail
    self.tail.prev = current

  # Set a list of hashtag_pair to the data structure
  # Return a list of timeout hashtag_pair
  def set_hashtag_pair_list(self, hashtag_pair_list, time):
    for hashtag_pair in hashtag_pair_list:
      self.set(hashtag_pair, time)
    return self.remove_old_hashtag_pair(time)

  # Remove timeout hashtag_pair in the data structure
  def remove_old_hashtag_pair(self, current_time):
    remove_list = []
    compare_time = current_time - timedelta(seconds = 60)
    current = self.head.next
    while (current != self.tail and current.val < compare_time):
      self.head.next = current.next
      current.next.prev = self.head
      remove_list.append(current.key)
      del self.cache[current.key]
      current = current.next
    return remove_list

  # Return a string represent the information stored in the data structure, for debug
  def list_to_string(self):
    result = ""
    if 0 != len(self.cache):
      current = self.head.next
      while current != self.tail:
        result += str(current.key) + ', (' + str(current.val) + ')\n'
        current = current.next
    return result