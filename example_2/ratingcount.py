from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        x = rating
        x = float(x)
        g = sum(HName) / sum(x)
        result = [HName, g]
        yield result

 


if __name__ == '__main__':
    MRHotelRaitingCount.run()
