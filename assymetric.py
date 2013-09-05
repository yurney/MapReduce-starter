import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    friend = record[1]
    friendship = []
    friendship.append(key)
    friendship.append(friend)
    mr.emit_intermediate(key, friendship)
    mr.emit_intermediate(friend, friendship)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    myFriends = []
    friendOf = []
    for v in list_of_values:
      if (key == v[0]):
        myFriends.append(v[1])
      if (key == v[1]):
	    friendOf.append(v[0])
    for friend in myFriends:
      isResult = True
      for other in friendOf:
        if (other == friend):
          isResult = False
      if (isResult):
	    mr.emit((key,friend))
	    mr.emit((friend,key))
 

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
