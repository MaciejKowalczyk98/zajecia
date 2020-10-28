from mrjob.job import MRJob
import os

class film(MRJob):
    def mapper(self, key, line):
        podz = []
        numK = 0
        if "ratings" in os.environ['map_input_file']:
            podz = line.split(',')
            numK = 1

        else:
            podz = line.split(',')
            numK = 0

        yield podz[numK], podz

    def reducer(self, film, value):
        wynik = []
        for x in value:
            wynik.append(x)

        if len(wynik) == 1:
            yield film, 'No Rating'

        else:
            try:
                to = [float(x[2]) for x in wynik[:-1]]

                if len(wynik[-1]) == 3:
                    title = wynik[-1][1]

                else:
                    title = ''.join(wynik[-1][1:-1])
                el = len(to)
                to = sum(to)
                yield film, {"title": title, "rating": to / el}

            except:
                pass

if __name__ == '__main__':film.run()
