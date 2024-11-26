import random

random.seed(2272100)

lst = []
for _ in range(2**16):
    lst.append(f"{random.randint(0, 100000)}")

with open("keys.csv", "w") as f:
    f.write("\n".join(lst))
