from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (x, z, x, b) = line.split(",")

        result = [z, 1]

        yield result

   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
