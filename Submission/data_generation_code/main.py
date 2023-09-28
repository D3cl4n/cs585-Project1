from accesslogs import LogsGeneration
from associates import Associates
from facein import FaceInData

# Generate all the data
def main():
    size = 200
    facein_handler = FaceInData(size)
    facein_handler.generateData()
    log_handler = LogsGeneration(size, "FaceIn.csv")
    log_handler.generate_data()

    associate_handler = Associates(size)
    associate_handler.generateData()

    with open("access.csv", "r") as f:
        data = f.read().splitlines(True)

    with open("access.csv", "w") as f_out:
        f_out.writelines(data[1:])

    f.close()
    f_out.close()

    with open("associates.csv", "r") as f:
        data = f.read().splitlines(True)

    with open("associates.csv", "w") as f_out:
        f_out.writelines(data[1:])

    f.close()
    f_out.close()

    with open("facein.csv", "r") as f:
        data = f.read().splitlines(True)

    with open("facein.csv", "w") as f_out:
        f_out.writelines(data[1:])

    f.close()
    f_out.close()

if __name__ == '__main__':
    main()
