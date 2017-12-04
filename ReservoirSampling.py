import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt


def rsampling_implementation1(stream, size):
    reservoir = []
    for i,element in enumerate(stream):
        #print i,element
        if i<size:
            reservoir.append(element)
        elif random.randint(1,i+1) <= size:
            reservoir[random.randint(0,size-1)] = element
        #print reservoir
    return reservoir

def rsampling_implementation2(stream, size):
    reservoir = []
    for i,element in enumerate(stream):
        #print i, element
        if i >=size:
            rand = random.randint(0,i)
        if i<size:
            reservoir.append(element)
        elif rand < size:
            reservoir[rand] = element
        #print reservoir
    return reservoir

def streamgenerator(limit):
    x = 1
    while x<=limit:
        yield x
        x+=1

if __name__ == "__main__":
    leng = [1000, 10000,100000]
    #leng = [1000]
    for k in leng:
        reservoirCollection1 = []
        reservoirCollection2 = []
        for i in range(1,k):
            stream1 = streamgenerator(100)
            stream2 = streamgenerator(100)
            size = 1
            reservoir1 = rsampling_implementation1(stream1, size)
            reservoir2 = rsampling_implementation2(stream2, size)
            reservoirCollection1.append(reservoir1)
            reservoirCollection2.append(reservoir2)
        nrc1 = np.asarray(reservoirCollection1)
        nrc2 = np.asarray(reservoirCollection2)
        plt.hist(nrc1, bins = range(min(nrc1), max(nrc1)+10,10))
        plt.savefig(str(k)+'tries_technique1.png')
        plt.hist(nrc2, bins = 'auto')
        plt.savefig(str(k)+'tries_technique2.png')
        
    
