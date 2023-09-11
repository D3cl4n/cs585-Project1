from accesslogs import LogsGeneration
from associates import Associates
from facein import FaceInData

# Generate all the data
def main():
    size = 10
    facein_handler = FaceInData(size)
    facein_handler.generateData()
    log_handler = LogsGeneration(size, "FaceIn.csv")
    log_handler.generate_data()

    associate_handler = Associates(size)
    associate_handler.generateData()

if __name__ == '__main__':
    main()