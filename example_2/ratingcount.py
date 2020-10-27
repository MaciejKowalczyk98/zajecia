from mrjob.job import MRJob


class MRHotelRaitingCount(MRJob):

    def mapper(self, _, line):
        (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet,
         UContinent, ReviewMonth, ReviewDay) = line.split("\t")
        x = rating
        x = float(x)
        
        wynik = [HName, x] // suma ocen poszczególnych hoteli    
        result = [HName, 1] // ilość poszczególnych hoteli 
        
        yield wynik
        yield result           
        
    def reducer(self, key, value):
        result = [key, sum(value)]
        yield result

if __name__ == '__main__':
    MRHotelRaitingCount.run()
