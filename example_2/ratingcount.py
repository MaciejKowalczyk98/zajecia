from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        og = line.readlines()[1:]
        (x, z, y, k) = og.split(",")


        result = [k, 1]

        yield result
   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
