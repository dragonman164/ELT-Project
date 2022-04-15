import pickle 

f = open("./case1.pickle","rb")
g = open("./case2.pickle","rb")
h = open("./case3.pickle","rb")
data1 = pickle.load(f)
data2 = pickle.load(g)
data3 = pickle.load(h)

print(data1)
print(data2)
print(data3)


