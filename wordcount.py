from mapreduce import MapReduce

#----------------------------------------

class WordCount(MapReduce):

    def mapper(self, _, line):
        for word in line.split():
            yield (word,1)

    def combiner(self, key, values):
            yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

#----------------------------------------

input = open("output1.txt").readlines()

output = WordCount.run(input)
for item in output:
    print(item)
