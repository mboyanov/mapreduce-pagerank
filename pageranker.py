__author__ = 'mboyanov'

from mrjob.job import MRJob, MRStep
from mrjob.compat import get_jobconf_value


l = 0.15

class MRWordFrequencyCount(MRJob):

    def configure_options(self):
        super(MRWordFrequencyCount, self).configure_options()

        self.add_passthrough_option(
            '--sink', dest='sink', default=0, type='int',
            help='number of iterations to run')

    def sinknode_mapper(self, _ , line):
        data = line.split()
        nodes = data[2:]
        pagerank = float(data[1])
        key = data[0]
        yield key, pagerank
        if len(nodes)==0:
            yield "sinknode", pagerank

    def sinknode_reducer(self, key, values):
        if key == 'sinknode':
            values = (float (value) for value in values)
            self.increment_counter("sink","sink", sum(values))
        else:
            yield key, 0

    def mapper_1(self,key, pagerank):
        yield self.ge
        # data = line.split()
        # nodes = data[2:]
        # pagerank = data[1]
        # key = data[0]
        # for node in nodes:
        #     yield node, float(pagerank)/len(nodes)
        # yield key, nodes

    def reducer_1(self, key, values):
        key_pr = l
        nodes=[]
        for value in values:
            if type(value) == list:
                nodes = value
            else:
                key_pr += (1-l) * value
        yield key, [key_pr, nodes]

    def mapper_2(self, key , line):
        pagerank = line[0]
        nodes = line[1]
        for node in nodes:
            yield node, float(pagerank)/len(nodes)
        yield key, nodes



    def steps(self):
        return [
            MRStep(mapper=self.sinknode_mapper, reducer = self.sinknode_reducer)
          ,  MRStep(mapper = self.mapper_1)


        ]
if __name__ == '__main__':
    MRWordFrequencyCount.run()