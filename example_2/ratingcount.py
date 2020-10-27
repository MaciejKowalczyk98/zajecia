from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (x, z, y, k) = line.split(",")[1:]

        result = [k, 1]

        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
