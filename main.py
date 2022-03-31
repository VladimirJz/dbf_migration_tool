
from asyncio.windows_events import NULL
import os
from tracemalloc import start
from unicodedata import decimal
from dbar import handler
directory='c:\\Users\\vjimenezv\Documents\Dev\sistema_azul\db_files'
import chardet
from pathlib import Path
import datetime
from datetime import datetime

start_at=datetime.now()

# 2000/500 = 11 min
READ_BLOCK=2000 # Num of Rows (block) to read from BDF file  on loop
COMMIT_BLOCK=500 # Size of rows to  commit to SQL database  from READ_BLOCK on loop

db_constring=("Driver=ODBC Driver 17 for SQL Server;"
            "Server=172.16.20.3;"
            "Database=SISTEMAAZUL;"
            "UID=sa;"
            "PWD=#1Qazse4")


files = Path(directory).glob('*.dbf')
source=handler.vfp_dbf()
target=handler.mssql(db_constring)
for file in files:
    table_name=file.name.upper().replace(".DBF",'')
    field_props=source.get_fields(file)
    #print(field_props)
    create_script=source.ddl_create_table(table_name,field_props,'PUNTERO','int')
    #print(create_script)
    print("loading data ...")
    num_fields=len(field_props[0])
    fields=",".join(field_props[0].keys())
    sql="INSERT INTO " + table_name + "(" + fields + ") VALUES (" + ",".join("?" * num_fields) + ");"    
    blocks=source.data_generator(file,READ_BLOCK)
    
    for block in blocks:
        print("INSERTANDO BLOQUE:")
        #print(block)
        print(target.insert_list(sql,block,COMMIT_BLOCK))
    end_at=datetime.now()
    print ("TERMINADO.")
    print("Tiempo de ejecución:",end_at  -start_at)
    print ("inicio:",start_at)
    print("end:",end_at)
     


    





    


# table=dbf.Table(filename='C:\\Users\\vjimenezv\\Downloads\\pagosCH.dbf')
# table.open()
# r=table.first_record
# names=table.structure()
# sup=table.supported_tables
# fl=table.field_names
# mt=table._MetaData()
# print(mt)
# print(fl)
# print(sup)
# print(r)
# #print(type(names))
# print(names)

# print(str(r) )
# for r in table:
# 	entero=int.from_bytes(r.Punteror,"big")
# 	print(entero)
	
	
# table.close()
