from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):

    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        x = rating
        x = float(x)
        
        wynik = [rating, 1]
        yield wynik    
       
        result = [HName, x]
        yield result
 
        
    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result
        wynik = [key, sum(value)]
        yield wynik
    


if __name__ == '__main__':
    MRHotelRaitingCount.run()
