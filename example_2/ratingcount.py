from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        result = [HName, 1]
        yield result
        
        
    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        
        wynik = [rating, 1]
        yield wynik
    
    def reducer(self, key, value):
        result = [key, sum(value)]  
        wynik = [key, sum(value)]
        yield result + wynik


if __name__ == '__main__':
    MRHotelRaitingCount.run()
