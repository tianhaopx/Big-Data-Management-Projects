#PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" pyspark
import math
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Project3").setMaster("local")
try:
    sc.stop()
    sc = SparkContext(conf=conf)
except:
    pass
sc = SparkContext.getOrCreate()

df = sc.textFile("hdfs://localhost/test/input/pto.csv")


def AssignCell(s):
    s = s.split(',')
    x = int(s[0])
    y = int(s[1])
    cell = round(x/20) + round(abs(y-9999)/20)*500+1
    if cell == 1:
        temp = []
        for i in [1, 2, 501, 502]:
            if i == 1:
                temp.append([i,"1,0,3"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif cell == 249501:
        temp = []
        for i in [249501, 249001, 249002, 249502]:
            if i == 249501:
                temp.append([i,"1,0,3"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif cell == 500:
        temp = []
        for i in [500, 499, 999, 1000]:
            if i == 500:
                temp.append([i,"1,0,3"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif cell == 250000:
        temp = []
        for i in [250000, 249499, 249500, 249999]:
            if i == 250000:
                temp.append([i,"1,0,3"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif (cell>1 and cell<500):
        temp = []
        for i in [cell, cell - 1, cell + 1, cell + 499, cell + 500, cell + 501]:
            if i == cell:
                temp.append([i,"1,0,5"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif (cell>249501 and cell<250000):
        temp = []
        for i in [cell, cell - 1, cell + 1, cell - 499, cell - 500, cell - 501]:
            if i == cell:
                temp.append([i,"1,0,5"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif (cell % 500 == 1):
        temp = []
        for i in [cell, cell - 500, cell - 499, cell + 1, cell + 500, cell + 501]:
            if i == cell:
                temp.append([i,"1,0,5"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    elif (cell % 500 == 0):
        temp = []
        for i in [cell, cell - 500, cell - 501, cell - 1, cell + 500, cell + 499]:
            if i == cell:
                temp.append([i,"1,0,5"])
            else:
                temp.append([i,"0,1,0"])
        return temp

    else:
        temp = []
        for i in [cell, cell - 1, cell + 1, cell - 501, cell - 500, cell - 499, cell + 501, cell + 500, cell + 499]:
            if i == cell:
                temp.append([i,"1,0,8"])
            else:
                temp.append([i,"0,1,0"])
        return temp

def SumCell(s1, s2):
    s1 = s1.split(",")
    s2 = s2.split(",")
    self_cell = int(s1[0]) + int(s2[0])
    relative_cel = int(s1[1]) + int(s2[1])
    relative_cell_count = 0
    if int(s1[2]) != 0:
        relative_cell_count = int(s1[2])
    elif int(s2[2]) != 0:
        relative_cell_count = int(s2[2])
    return str(self_cell)+","+str(relative_cel)+","+str(relative_cell_count)

Coor = df.flatMap(AssignCell)
Sum_Cell = Coor.reduceByKey(SumCell)

def CalDensity(s):
    cell = s[0]
    info = s[1].split(',')
    density = float(info[0]) * float(info[2]) / float(info[1])
    return [int(cell),density]

idx = Sum_Cell.map(CalDensity)
ans = idx.sortBy(lambda x:x[1], ascending=False)

ans = ans.take(50)
density = {}
for n in ans:
    density[n[0]] = n[1]
Top_density = sc.broadcast(density)



def Neighbor_density(s):
    cell = int(s[0])
    density = float(s[1])

    if cell == 1:
        temp = []
        for i in [2, 501, 502]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif cell == 249501:
        temp = []
        for i in [249001, 249002, 249502]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif cell == 500:
        temp = []
        for i in [499, 999, 1000]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif cell == 250000:
        temp = []
        for i in [249499, 249500, 249999]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif (cell>1 and cell<500):
        temp = []
        for i in [cell - 1, cell + 1, cell + 499, cell + 500, cell + 501]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif (cell>249501 and cell<250000):
        temp = []
        for i in [cell - 1, cell + 1, cell - 499, cell - 500, cell - 501]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif (cell % 500 == 1):
        temp = []
        for i in [cell - 500, cell - 499, cell + 1, cell + 500, cell + 501]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    elif (cell % 500 == 0):
        temp = []
        for i in [cell - 500, cell - 501, cell - 1, cell + 500, cell + 499]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

    else:
        temp = []
        for i in [cell - 1, cell + 1, cell - 501, cell - 500, cell - 499, cell + 501, cell + 500, cell + 499]:
            if i in Top_density.value:
                temp.append([i,str(cell)+","+str(density)])
        return temp

def Aggergate_Neighbor(s1, s2):
    return s1 + "," + s2


neighbor_density = idx.flatMap(Neighbor_density)
neighbor_density = neighbor_density.reduceByKey(Aggergate_Neighbor)
neighbor_density.collect()
