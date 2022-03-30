
from asyncio.windows_events import NULL
import os
from unicodedata import decimal
from dbar import handler
directory='c:\\Users\\vjimenezv\Documents\Dev\sistema_azul\db_files'
import chardet
from pathlib import Path
import datetime

db_constring=("Driver=ODBC Driver 17 for SQL Server;"
            "Server=172.16.20.3;"
            "Database=AZUL;"
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
    dict_rows=source.get_data(table_name,file)
    #print(type(dict_rows))
    list_values=[]
    for  row  in dict_rows:
        row_values=[]
        for key,value in row.items():
            #print([key])
            #print([value])
            #print(type(value))
            casted_value=value

            if not value:
                casted_value=None

            if( isinstance(value,bytes)):
                
                
                try:
                    if source.cast_patern in key:
                        casted_value=int.from_bytes(value,"big")
                    else:
                        casted_value=value.decode('UTF-8') # ascii
                except:
                    #char_set=chardet.detect(value)
                    #print ("char",char_set)
                    casted_value=value.decode('ISO-8859-1') 

            if(isinstance(value,datetime.date)):
                casted_value=str(value.isoformat())

            if(isinstance(value,bool)):
                if(value):
                    casted_value=1
                else:
                    casted_value=0
            
            if(isinstance(casted_value,str)):
                if(casted_value.count("'")>1):
                    #print("LOCATED !!!!")
                    if( "".join(casted_value.split())=="b''"):
                        #print("FIXED")
                        casted_value=None


            # if(casted_value=="b' '"):
            #     casted_value=""
            # if(casted_value=="b'  '"):
            #     casted_value=""
            # if(casted_value=="b'    '"):500
            #     casted_value=""
            # if(casted_value=="b'          '"):
            #     casted_value=""

            if(isinstance(value,float)):
                casted_value=value*1
       

            #print("cast:",casted_value)

            #if(isinstance(value,str)):
            #    casted_value=value.removeprefix("b'  '")

            row_values.append(casted_value)
        row_values=tuple(row_values)
        list_values.append(row_values)
    #print(list_values)
    #print('Terminado')
    #print(field_props)
    num_fields=len(field_props[0])
    fields=",".join(field_props[0].keys())
    sql="INSERT INTO " + table_name + "(" + fields + ") VALUES (" + ",".join("?" * num_fields) + ");"
   
    # 154,520
    # 
    #    # for set in list_values:
    #    if()
    #    sql2="INSERT INTO " + table_name + "(" + fields + ") VALUES (" + ",".join(set) + ");"
    #print (sql)
    print(target.insert_list(sql,list_values,2000))
     


    





    


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
