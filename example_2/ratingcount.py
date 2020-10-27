from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (movield, title, genres, x) = line.split(",")

        result = [title]

        yield result

   


if __name__ == '__main__':
    MRHotelRaitingCount.run()
