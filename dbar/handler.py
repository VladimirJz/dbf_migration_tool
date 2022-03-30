from dataclasses import dataclass
from operator import length_hint
import dbf
import pyodbc

import os
import ast
from pathlib import Path
import datetime




class vfp_dbf():
    # https://docs.microsoft.com/en-us/sql/odbc/microsoft/visual-foxpro-field-data-types?view=sql-server-ver15
    datatypes={'C':'VARCHAR',
                'N':'DECIMAL',
                'D':'DATE',
                'I':'INT',
                'L':'NVARCHAR', #for testing
                }
    _cast_patern=""
    _cast_datatype=""

    @property
    def cast_patern(self):
        return self._cast_patern
    
    @cast_patern.setter
    def cast_patern(self,value):
        self._cast_patern = value

    @property
    def cast_datatype(self):
        return self._cast_datatype
    
    @cast_patern.setter
    def cast_datatype(self,value):
        self._cast_datatype = value


    def get_fields(self,file):
        #print(file)
        file_name=os.path.normpath(str(file.absolute()))
        table_name=(file.name).replace(".dbf","")
        #print(table_name)
        #table=dbf.Table(filename=file_name)
        table=dbf.Table(filename=file_name)
        table.open()
        fields_list=list(table.structure())
        fields=self._to_dic(fields_list," ")
        table.close()
        return(fields)
    
    def get_data(self,tablename,file):
        file_name=os.path.normpath(str(file.absolute()))
        #print(table_name)
        table=dbf.Table(filename=file_name)
        table.open()
        #print(table.first_record())
        #print("echo")
        # table_rows=list(table)
        #print(table_rows)
        i=0
        data=[]
        for row in table:
            #print("Table: ",tablename)
           # print(row)
            #print(row.to_list())
            data.append(row.to_list())
            # i=i+1
            # if i==100000:
            #     break
        #print("Load data finished")
        table.close()
        return data
    
    def data_generator(self,table,blocksize):
        def block_generator(table):                
            for row in table:
                yield row.to_list()
            pass

        data_block=block_generator(table)
        data=[]
        for item in data_block:
            print("loop over items ")
            data.append(item)
            if(len(data)>blocksize):
                print("Yield the list")
                yield data
                data=[]
       




def sanitize(self,dict_rows):
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
                    if self._cast_patern in key:
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
        return list_values





























    def load_data(self,tablename,file,block_size,commit_size):
        file_name=os.path.normpath(str(file.absolute()))
        #print(table_name)
        data=[]
        table=dbf.Table(filename=file_name)
        table.open()
        # data_stream=self.data_generator(table)
        # for row in data_stream:
        #     data.append(row)
        #     if len(data)>block_size:

            

        #print(table.first_record())
        #print("echo")
        # table_rows=list(table)
        #print(table_rows)
        i=0
        data=[]
        for row in table:
            #print("Table: ",tablename)
           # print(row)
            #print(row.to_list())
            data.append(row.to_list())
            # i=i+1
            # if i==100000:
            #     break
        #print("Load data finished")
        table.close()
        return data

    def get_numrows(self,tablename,file):
        file_name=os.path.normpath(str(file.absolute()))
        #print(table_name)
        table=dbf.Table(filename=file_name)
        table.open()
        last=table.last_record
        #print(last)
        num_rows=table.index(last)
        #table.last_record()
        #position=table.goto('bottom')
        table.close()
        return num_rows


    
    def ddl_create_table(self,tablename,fields,cast_fieldpattern=None,cast_todatatype=None):
        '''
        tablename (str):Table name
        fields (dict): Table fields specifications
        map_field_pattern (str): Cast the type for fields than match whit str pattern.
        cast_to_datatype(str): Cast the original datatye for the given.
        '''
        field_definition=[]
        script="CREATE TABLE " + tablename
        if(cast_fieldpattern):
            self.cast_patern=cast_fieldpattern
            self._cast_datatype=cast_todatatype

        for field in fields:
            #print (field)
            for key in field:
                field_name=key
                specs=field[key]

                source_datatype=specs[:1]
                try:
                    datalen=specs[1:specs.index(")")+1]
                except:
                    datalen="" 
                    pass
                datatype=self.datatypes[source_datatype]
                if cast_fieldpattern in field_name:
                    datatype=cast_todatatype
                    datalen=""

                definition= field_name + ' '  + datatype + datalen
                field_definition.append(definition)

        script=script + "(" + ','.join(field_definition) + ");"
        return script

            
        

    def _to_dic(self,list,separator):
        result = [{}]
        for item in list:
            key, val = item.split(separator, 1)
            if key in result[-1]:
                result.append({})
            result[-1][key] = val
        return (result)
# MSSQL
class mssql():
    def __init__(self,stringconnection):
        self.string_connection=stringconnection
        self._connection=self.get_connection()
   
    @property
    def string_connection(self):
        return self._string_connection
    
    @string_connection.setter
    def string_connection(self,value):
        self._string_connection = value

    def get_connection(self):
        #print (self._string_connection)
        db_connection=pyodbc.connect(self._string_connection)
        return db_connection
    
    def insert_list_bak(self,dml_script,list_values,block_size):
        cursor=self._connection.cursor()
        insert_block=list()
    
        print("Len:",len(list_values))
        for i in range(0,len(list_values),block_size):
            insert_block.append(list_values[i:i+block_size])
            print (insert_block)
            cursor.executemany( dml_script,insert_block)
            cursor.commit()
        cursor.close()

    
    def insert_list(self,dml_script,list_values,commit_size):
        cursor=self._connection.cursor()
        
        num_rows=len(list_values)
        start=0
        end=0
        for i in range(0,num_rows,commit_size):
            end=end+commit_size
            if end>num_rows:
                end=num_rows
            #print ("De:",start, " to ", end)
            insert_block=list_values[start:end]
            try:
                cursor.executemany( dml_script,insert_block)
                cursor.commit()
            except:
                print ("Err: Block insert fail")
                print("Index:",start," to: ", end)
                print(print (insert_block))
                break
            print("Insert rows:",start," to: ", end)
            start=end #+1
            # self._connection.commit()
            #print (insert_block)
        cursor.close()
        return "Terminado"
        #temp=list_values[0:2]
  
        #for i in range(0,len(list_values[0]),block_size):
            #print (insert_block)
       # cursor.close()

# db_constring=("Driver=ODBC Driver 17 for SQL Server;"
#             "Server=172.16.20.3;"
#             "Database=AZUL;"
#             "UID=sa;"
#             "PWD=#1Qazse4")



#  directory='c:\\Users\\vjimenezv\Documents\Dev\sistema_azul\db_files'



#  files = Path(directory).glob('*.dbf')

#  for file in files:
#      h=vfp_dbf()    
#      print(h.get_numrows('',file):
