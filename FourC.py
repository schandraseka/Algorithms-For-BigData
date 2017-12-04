from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import statistics


class TwoHops(MRJob):

    def steps(self):
        return [
        MRStep(mapper=self.mapper, combiner = self.combiner, reducer=self.reducer),
	MRStep(reducer=self.reducer1)
        ]
    
    def mapper(self, _, record):
        fromto = record.split('\t')
        sender = fromto[0].strip()
        receiver = fromto[1].strip()
        yield (int(sender), [int(receiver),1])
        yield (int(receiver), [int(sender),0])

    
    def combiner(self, key,vals):
        inedges = [] 
        outedges = [] 
        for vl in vals:	                   
            if vl[1] == 0:
                inedges.append(vl[0])
            else:
                outedges.append(vl[0])       
        for inedge in inedges:
            yield (inedge, outedges)


    def reducer(self, key, vals):
        lis = []
        for val in vals:
            lis= lis+val
        lis = list(set(lis))
        print("Key = "+str(key)+ " "+str(lis))
        yield None, (key,len(lis)) 

    def reducer1(self, _,vals):
        accessible = []
        res = []
        number_of_nodes = 265214
        for val in vals:
            accessible.append(val[0])    
            res.append(val[1])
        for i in range(number_of_nodes-len(accessible)):
            res.append(0)
        yield "Mean 2 hops", sum(res)/len(res)
        yield "Median 2 hops", statistics.median(res)

	



if __name__ == '__main__':
    TwoHops.run()
