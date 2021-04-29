def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)
print(updateFunc([1,2,3,4],11))