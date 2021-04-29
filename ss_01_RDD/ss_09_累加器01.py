counter = 0
data=(range(10))

def increment(x):
    global counter
    counter += x

for i in data:
        print(i)
        increment(i)
print("Normal Counter value: ", counter)
