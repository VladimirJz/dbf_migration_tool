from dbar import handler
from pathlib import Path
import os
import dbf


directory='c:\\Users\\vjimenezv\Documents\Dev\sistema_azul\db_files'



files = Path(directory).glob('*.dbf')
for file in files:
    file_name=os.path.normpath(str(file.absolute()))
#print(table_name)
    table=dbf.Table(filename=file_name)
    table.open()
    db=handler.vfp_dbf()
    blocks=db.data_generator(table,500)
    for  block in blocks:
        print("Block de :",len(block))
        print(block)