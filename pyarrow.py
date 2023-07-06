#create a python program, that given a json with an memory addresses and iterations
#turn it into a paraquet. Given a paraquet with the same nature, turn it into a json



import pyarrow as PyA
import json
import sys

def writeToParaquet(inFile: str, outFile: str):
    import pandas as Pa
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
        columnAddress.append(obj["iteration"])
    
    #we now have our columns, so lets make the table
    #we will use a pandas data frame


    dataFrame = Pa.DataFrame({
        "address" : columnAddress,
        "iteration" : columnIteration
    })
#add_column
    table = PyA.Table.from_pandas(dataFrame)
    PyA.parquet.write_table(table, outFile)
    print(f"Wrote {inFile} to {outFile}")


#TODO reverse a paraquet back into json




if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python ./pyarrow.py inFile.json outFile.parquet")
        exit(1)
    
    #TODO check to see filenames

    writeToParaquet(sys.argv[1], sys.argv[2])
