from mrjob.job import MRJob
import pandas as pd



class MRHotelRaitingCount(MRJob):
    a = pd.read_csv("movies.csv")
    b = pd.read_csv("ratings.csv")

    df3 = a.merge(b, on=["movieId"], how='outer')
    df3.to_csv("final.csv", index=False)
    def mapper(self, _, line):
        (a, ty, d, us, mo, rating, tim) = line.split("\t")

        x = rating
        x = float(x)

        result = [ty, x]

        yield result




if __name__ == '__main__':
    MRHotelRaitingCount.run()
