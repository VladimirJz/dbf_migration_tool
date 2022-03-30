from dbar import handler
from pathlib import Path
from sys import os
import dbf


directory='c:\\Users\\vjimenezv\Documents\Dev\sistema_azul\db_files'



files = Path(directory).glob('*.dbf')
for file in files:
    file_name=os.pathnormpath(str(file.absolute()))
#print(table_name)
    table=dbf.Table(filename=file_name)
    table.open()
    db=handler.vfp_dbf()
    blocks=db.data_generator(table,2)
    for  block in blocks:
        print("Block")