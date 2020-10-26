from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        g = rating
        g = float(x)
        result = [HName, g]
        yield result

    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result


if __name__ == '__main__':
    MRHotelRaitingCount.run()
