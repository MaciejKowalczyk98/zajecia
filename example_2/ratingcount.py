from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    file_name = os.environ['mapreduce_map_input_file']
    print  (file_name)

if __name__ == '__main__':
    MRHotelRaitingCount.run()
