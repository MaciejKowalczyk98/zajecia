from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (a, s, d, f)  = line.replace(' ','').replace('N/A','').split(',')
        result = [s, 1]

        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
