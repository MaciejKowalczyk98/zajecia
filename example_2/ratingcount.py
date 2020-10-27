from mrjob.job import MRJob
import csv


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        cos = csv.reader(line)
        next(cos)
        (a, s, d, f)  = line.split(",")
        
        result = [s, 1]
        
        


        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
