from mrjob.job import MRJob
import csv


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (a, s, d, f)  = line.split(",")
        cos = csv.reader(line)
        next(cos)
        result = [cos, 1]
        
        


        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
