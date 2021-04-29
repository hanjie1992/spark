f = "../data/streaming/logfile.txt"
data=['test1','test2','test3','hello','hello']
with open(f,"w") as file:
    for i in data:
        file.write(i)
        file.write('\n')
file.close()
