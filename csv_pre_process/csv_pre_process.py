class CSV:

    def __init__(self, file):
        self.file = file
        self.headers = []
        self.headerMaxLen = []
        self.mainArray = []

    def read(self):
        count = 0
        # Split file by line
        lines = self.file.read().splitlines()

        # Building header first
        first_line = lines[0]
        for header in first_line.split(","):
            self.headers.append(header)
            self.headerMaxLen.append([])
            self.mainArray.append([])

        # read all data
        for i in range(1, len(lines)):
            for word in enumerate(lines[i].split(",")):
                self.mainArray[word[0]].append(word[1])

            count = count + 1

        # Compute max length for each header
        for i in range(0, len(self.mainArray)):
            maxLen = 0
            for record in self.mainArray[i]:
                if len(record) > maxLen:
                    maxLen = len(record)
            self.headerMaxLen[i] = maxLen
        print(self.headers)
        print(self.headerMaxLen)
        print(count)

    def padding(self):
        # for each record doesn't reach the max length pad it to the max length
        for colIdx in range(0, len(self.mainArray)):
            thisColMaxLength = self.headerMaxLen[colIdx]
            for recordIdx in range(0,len(self.mainArray[colIdx])):
                numToPad = thisColMaxLength - len(self.mainArray[colIdx][recordIdx])
                #print(numToPad)
                if numToPad > 0:
                    paddingString = ""
                    for i in range(0, numToPad):
                        paddingString = paddingString + " "
                    self.mainArray[colIdx][recordIdx] = self.mainArray[colIdx][recordIdx] + paddingString
                    #print(self.mainArray[colIdx][recordIdx])

    def writeToFile(self, outputFile):
        # Write header back first
        # line = ""
        # for header in self.headers:
        #     line = line + header + ","
        #
        # outputFile.write(line[:(len(line)-1)] + "\n")

        # Write each data back
        numRecords = len(self.mainArray[0])
        for recordIdx in range(0, numRecords):
            line = ""
            for colIdx in range(0, len(self.mainArray)):
                line = line + self.mainArray[colIdx][recordIdx] + ","
            outputFile.write(line[:(len(line)-1)] + "\n")



if __name__ == '__main__':

    input_file_name = "s.csv"
    output_file_name = "s.csv.out"

    input_file = open(input_file_name, "r")
    output_file = open(output_file_name, "w")

    csv = CSV(input_file)
    csv.read()
    csv.padding()
    csv.writeToFile(output_file)

