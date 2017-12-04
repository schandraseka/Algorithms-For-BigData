import random
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def hashfunctiongenerator(prime_seed, max_value):
	a = random.randint(1,prime_seed-1)
	b = random.randint(0, prime_seed-1)
	return lambda x: ((a*x + b) % prime_seed) % max_value
	
	
def streamgenerator(stream_size, max_value):
    x = 1
    lst = []
    while x<=stream_size:
        val = random.randint(1,max_value)
        lst.append(val)
        x+=1
    return lst
        
stream_size = 1000000
max_value = 1000
stream = streamgenerator(stream_size, max_value)
epsilons = [0.01, 0.001,0.0001]
df = pd.DataFrame({'index':list(range(1,max_value+1))})
plt.figure(2, figsize=(6,4))
for epsilon in epsilons:
	d = 25
	prime_seed = 32452843
	w = math.floor(math.exp(1)/epsilon)
	hashfamily = []
	#Deciding on the p hash functions
	for i in range(d):
		hashfunction = hashfunctiongenerator(prime_seed, w+1)
		hashfamily.append(hashfunction)
	#Array for count min sketch
	a = [[0]*(w+1)]*d
	freq_count = [0]*(max_value+1)
	for value in stream:
		#Keeping the actual value for plotting
		freq_count[value] +=1
		for i in range(d):
			a[i][hashfamily[i](value)] = a[i][hashfamily[i](value)] + 1
	actual = []
	count_min = []
	for value in range(1,max_value+1):
		actual.append(freq_count[value])
		count = stream_size
		for i in range(d):
			count = min(count, a[i][hashfamily[i](value)])
		count_min.append(count)
	df[str(epsilon)] = count_min
plt.plot(df['index'],df['0.01'],'or-', linewidth=3) 
plt.plot(df['index'],df['0.001'],'sb-', linewidth=3) 
plt.plot(df['index'],df['0.0001'], 'pm-',linewidth=3)
plt.plot(df['index'],actual, 'xy-',linewidth=3)

plt.grid(True) #Turn the grid on
plt.ylabel("Count min Frequency") #Y-axis label
plt.xlabel("Actual frequency") #X-axis label
plt.title("Actual frequency v/s Count min frequency") #Plot title
plt.savefig("count_min_plot.pdf")
