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
    orderId = record[1]
    mr.emit_intermediate(orderId,record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    lineItemList = []
    orderList = []
    for e in list_of_values:
      if (e[0] == 'line_item'):
        lineItemList.append(e)
      if (e[0] == 'order'):
        orderList.append(e)
    for lines in lineItemList:
      for orders in orderList:
        mr.emit(orders+lines)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
