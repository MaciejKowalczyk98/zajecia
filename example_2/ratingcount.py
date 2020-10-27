from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (movield, title, genres, x) = line.split(",")

        result = [title, 1]

        yield result

   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
