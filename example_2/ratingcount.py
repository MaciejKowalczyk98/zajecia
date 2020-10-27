from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (x, z, x, b) = f.readline()

        result = [z, 1]

        yield result

   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
