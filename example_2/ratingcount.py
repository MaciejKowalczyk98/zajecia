from mrjob.job import MRJob
import csv


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (a, s, d, f)  = line.split(",")
        csv_reader = csv.reader(line)
        next(csv_reader)
        result = [s, 1]
        
        


        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
