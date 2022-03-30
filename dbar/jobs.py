from operator import length_hint
import dbf
import os
import ast


class dbf():
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
        data=[]
        for row in table:
            #print("Table: ",tablename)
           # print(row)
            print(row.to_list())
            data.append(row.to_list())
        #print("Load data finished")
        table.close()
        pass


    
    def create_table(self,tablename,fields,cast_fieldpattern=None,cast_todatatype=None):
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

        



        pass
        


        

    def _to_dic(self,list,separator):
        result = [{}]
        for item in list:
            key, val = item.split(separator, 1)
            if key in result[-1]:
                result.append({})
            result[-1][key] = val
        return (result)

class mssql()
    pass