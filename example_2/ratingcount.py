from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        g = rating
        g = float(x)
        s = sum(x) / len(x)
        y = str(round(s, 2))
        result = [HName, y]
        yield result

    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result


if __name__ == '__main__':
    MRHotelRaitingCount.run()
