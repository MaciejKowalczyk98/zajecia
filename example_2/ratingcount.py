from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):
    def mapperr(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
       
        
       
        result = [rating, 1]
        yield result
 
        
    def reducerr(self, key, value):
        result = [key, sum(value)]
        yield result

    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        x = rating
        x = float(x)
        
       
        result = [HName, x]
        yield result
 
        
    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result
        
    


if __name__ == '__main__':
    MRHotelRaitingCount.run()
