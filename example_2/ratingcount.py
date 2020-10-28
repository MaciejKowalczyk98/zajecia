from mrjob.job import MRJob
import os

class MRHotelRaitingCount(MRJob):
    def mapper(self, key, line):
        splits = []
        idKey = 0
        if "ratings" in os.environ['map_input_file']:
            splits = line.split(',')
            idKey = 1
        else:
            splits = line.split(',')
            idKey = 0
        yield splits[idKey], splits

    def reducer(self, movieID, value):
        a = []
        for x in value:
            a.append(x)

        if len(a) == 1:
            yield movieID, 'No Rating'
        else:
            total = 0
            elements = 0
            if len(a[-1]) == 3:
                title = a[-1][1]
            else:
                title = ''.join(a[-1][1:-1])
            for x in a[:-1]:
                k = float(x[2)
                total += k
                elements += 1

            yield movieID, {"title": title, "rating": total / elements}

if __name__ == '__main__':
    MRHotelRaitingCount.run()
