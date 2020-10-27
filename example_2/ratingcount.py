from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (movield, title, genres) = line.split("\t")

        result = [title, 1]

        yield result

    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result


if __name__ == '__main__':
    MRHotelRaitingCount.run()
