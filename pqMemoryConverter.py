#create a python program, that given a json with an memory addresses and iterations
#turn it into a paraquet. Given a paraquet with the same nature, turn it into a json



from pyarrow import (parquet)
import json
import sys

def writeToParaquet(inFile: str, outFile: str):
    import pyarrow
    #first, open the json file
    inF = open(inFile)

    #Data should look like this
    #[{address: int, iteration: int},
    # {address: int, iteration: int}]
    #We need to change this into a two column table

    columnAddress = []
    columnIteration = []
    
    jsonData = json.load(inF)
    
    assert (type(jsonData) == list)
    
    for obj in jsonData:
        columnAddress.append(obj["address"])
        columnIteration.append(obj["iteration"])
    
    #we now have our columns, so lets make the table

    dataDict = {
        "address": columnAddress,
        "iteration": columnIteration
    }

    table = pyarrow.Table.from_pydict(dataDict)

    parquet.write_table(table, outFile)
    print(f"Wrote {inFile} to {outFile}")

#TODO reverse a paraquet back into json
def writeToJson(inFile: str, outFile: str):
    import pyarrow
    #get the arrow table from the parquet file
    table = parquet.read_table(inFile)

    #convert it to a python dictionary
    dataDict = table.to_pydict()

    jsonData = []

    #now, iterate through dataDict and reconstruct the original json file
    for i in len(dataDict["address"]): 
        jsonData.insert(0, {"address" : dataDict["address"][i], 
                            "iteration" : dataDict["iteration"][i]})
    outF = open(outFile, "w")
    json.dump(outF)



def readParquet (filename: str):
    fileData = parquet.read_table(filename)
    print(fileData)





if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python ./pqMemoryConverter.py inFile.json outFile.parquet")
        exit(1)
    
    #TODO check to see filenames

    writeToParaquet(sys.argv[1], sys.argv[2])
    readParquet(sys.argv[2])
    writeToJson(sys.argv[2], "tempCompare.json")

