from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (a, s, d, f, g, h, j, k)  = line.split(",")
        result = [s, 1]

        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
