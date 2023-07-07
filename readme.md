This is a data converter built using pyarrow (pip install pyarrow) to convert a .json to a .parquet of PlayableQuote memory addresses. A .exe is included in ./dist for ease of use, generated with PyInstaller (pip install pyinstaller) (python -m PyInstaller --one-file pqMemoryConverter.py)

usage: python ./pqMemoryConverter.py data.json data.parquet
usage: ./dist/pqMemoryConverter.exe data.json data.parquet