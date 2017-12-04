from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import statistics


class NodesStats(MRJob):

    def steps(self):
        return [
        MRStep(mapper=self.mapper, reducer=self.reducer),
	MRStep(reducer=self.reducer1)
        ]
    
    def mapper(self, _, record):
        fromto = record.split('\t')
        sender = fromto[0].strip()
        receiver = fromto[1].strip()
        yield (sender, [0,1])
        yield (receiver, [1,0])
    
    def reducer(self, key,vals):
        incount = 0 
        outcount = 0 
        for vl in vals:	                   
            if vl[0] == 1:
                incount +=1
            elif vl[1] == 1:
                outcount +=1          
        yield None,[key,incount, outcount]
    
    def reducer1(self, _,vals):
        incounts = []
        outcounts = []
        indegreeThreshold = 100
        indegreeThresholdCount = 0        
        for vl in vals:
            incounts.append(vl[1])
            if vl[1] > indegreeThreshold:
                indegreeThresholdCount +=1
            outcounts.append(vl[2])
        yield "Count", len(incounts)
        yield "IncountAverage", sum(incounts)/len(incounts)
        yield "OutcountAverage", sum(outcounts)/len(outcounts)
        yield "IncountMedian", statistics.median(incounts)
        yield "OutcountMedian", statistics.median(outcounts)	
        yield "InDegree > 100", indegreeThresholdCount

	



if __name__ == '__main__':
    NodesStats.run()
