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
    matrix = record[0]
    rowId = record[1]
    colId = record[2]
    value = record[3]
    if (matrix == "a"):
      mr.emit_intermediate(str(rowId)+str(0), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(rowId)+str(1), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(rowId)+str(2), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(rowId)+str(3), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(rowId)+str(4), (matrix, rowId, colId, value))
    if (matrix == "b"):
      mr.emit_intermediate(str(0)+str(colId), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(1)+str(colId), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(2)+str(colId), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(3)+str(colId), (matrix, rowId, colId, value))
      mr.emit_intermediate(str(4)+str(colId), (matrix, rowId, colId, value))

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    rowId = int(key[0])
    colId = int(key[1])
    totalValue = 0
    sublist = list_of_values
    vVal = 0
    wVal = 0
    for v in list_of_values:
      matrixVal = v[0]
      rowVal = v[1]
      colVal = v[2]
      vVal = v[3]
      for w in list_of_values:
        if (v[0] == "a"):
          if ((w[0] == "b") and (w[1] == colVal)):
            wVal = w[3]
        else:
          if ((w[0] == "a") and (w[2] == rowVal)):
            wVal = w[3]
      totalValue+=(vVal*wVal)
    mr.emit((rowId, colId, totalValue))
    #for v in list_of_values:
    #  rValue = v[1]
    #  cValue = v[2]
    #  matrix = v[0]
    #  sublist.remove(v)
    #  for w in sublist:
    #    if (matrix != w[0] and cValue == w[1] and rValue == w[2]):
    #      sublist.remove(w)
    #      totalValue+=w[3]*v[3]
    #mr.emit((rowId, colId, totalValue))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
