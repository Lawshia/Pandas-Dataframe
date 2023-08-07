#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------#
# Authors: Lawshia Prabath
# Purpose: To replicate the core functionalities of the pandas DataFrame library
# Date:23-April-2023
# Language: Python
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------# 
from collections import defaultdict

class ListV2:
# Class which perfoms operations on list inputs      
    def __init__(self,l):
        self.values=list(l)
        if type(self.values) != list and type(self.values) != tuple:
            raise ValueError('The input is not a list or tuple')
        self.stop = len(self.values)
    def __iter__(self):
        self.value = 0
        return self 
    def __next__(self):
        if self.value < self.stop:
            self.return_value = self.values[self.value]
            self.value+=1
            return self.return_value
        else:
            raise StopIteration 
    def __add__(self, right):
        try:
            output = [l + r for l, r in zip(self.values, right)]
            return ListV2(output)
        except:
            if type(right) == int or type(right) == float:
                output = [l + right for l in self.values]
                return ListV2(output)
            elif type(right) == list or type(right) == tuple:
                output = [l + r for l,r in zip(self.values,right)]
                return ListV2(output)
            else:
                raise ValueError('String is not a valid operand')
    def __sub__(self, right):
        try:
            output = [l - r for l, r in zip(self.values, right)]
            return ListV2(output)
        except:
            if type(right) == int or type(right) == float:
                output = [l - right for l in self.values]
                return ListV2(output)
            elif type(right) == list or type(right) == tuple:
                output = [l - r for l,r in zip(self.values,right)]
                return ListV2(output)
            else:
                raise ValueError('String is not a valid operand')
    def __mul__(self, right):
        try:
            output = [l * r for l, r in zip(self.values, right)]
            return ListV2(output)
        except:
            if type(right) == int or type(right) == float:
                output = [l * right for l in self.values]
                return ListV2(output)
            elif type(right) == list or type(right) == tuple:
                output = [l * r for l,r in zip(self.values,right)]
                return ListV2(output)
            else:
                raise ValueError('String is not a valid operand')
    def __truediv__(self, right):
        try:
            output = [round(l / r,2) for l, r in zip(self.values, right)]
            return ListV2(output)
        except:
            if type(right) == int or type(right) == float:
                output = [round(l / right,2) for l in self.values]
                return ListV2(output)
            elif type(right) == list or type(right) == tuple:
                output = [round(l / r,2) for l,r in zip(self.values,right)]
                return ListV2(output)
            else:
                raise ValueError('String is not a valid operand')

    def mean(self):
        return sum(self.values)/len(self.values)
    def append(self,item):
        self.values.append(item)
        return ListV2(self.values)
    def __repr__(self):
        return str(self.values)
    pass



class DataFrame:
    def __init__(self,data,columns):
        self.data=data
        self.columns=columns
        self.list_data={}
        if type(self.data[-1]) == dict :   
            self.index={ele: i for i,ele in enumerate(self.data[-1]['yes_flag'])}         
            self.data.pop() 
        else:      
            self.index={ele: i for i,ele in enumerate([ele for ele in range(len(self.data))])}
            
        if type(self.data[-1]) == dict:
            self.data.pop()
        else:
            pass

        self.list_data.update({self.columns[ele]:[[self.data[ele2][ele] for ele2 in range(len(self.data))] for ele in range(len(self.columns))][ele] for ele in range(len(self.columns))})
        self.index=self.index
        
    def __repr__(self):
        if type(self.data[-1]) == dict:
            self.data.pop()
        else:
            pass
        data_line2=[list(self.columns)] + self.data
        for ele in range(len(data_line2)):          
            if ele == 0:
                data_line2[ele].insert(0,'')
            else:                
                data_line2[ele].insert(0,list(self.index.keys())[ele-1])
        return '\n'.join([','.join(map(str, item)) for item in data_line2])


        
    def iteritems(self):
        self.list_data={}
        self.list_data.update({self.columns[ele]:[[self.data[ele2][ele] for ele2 in range(len(self.data))] for ele in range(len(self.columns))][ele] for ele in range(len(self.columns))})
        return self.list_data
    
    def set_index(self,index_list):
        index_dict = {'yes_flag':index_list}
        self.data.append(index_dict)
        self.index = {ele: i for i,ele in enumerate(index_list)}
        return DataFrame(self.data,self.columns)

    def __setitem__(self, key, value): 
        self.list_data[key] = value
 
    def __getitem__(self, key): 
        if isinstance(key, str):
            return ListV2(self.list_data[key])  
        elif isinstance(key, list):
            output = []
            for k in key:
                output.append(self.list_data[k])
            output = list(zip(*output))
            output = [list(ele) for ele in output]
            return DataFrame(output,key)
        elif isinstance(key, slice):
            all_rows = list(zip(*self.list_data.values()))           
            output = all_rows[key.start:key.stop:key.step]
            output = [list(ele) for ele in output]
            return DataFrame(output,list(self.list_data.keys()))
        
        elif isinstance(key, tuple):
            row = key[0]
            col = key[1]            
            columns_name = list(self.list_data.keys())
            columns_name = columns_name[col]
            data = []
            for col in columns_name:
                data.append(self.list_data[col])
            
            all_rows = list(zip(*data))
            output = all_rows[row.start:row.stop:row.step]
            output = [list(ele) for ele in output]
            return DataFrame(output,columns_name)
 
    def drop(self,column_name):
        if type(self.data[-1]) == dict:
            self.data.pop()
        else:
            pass
        self.columns_list = self.columns
        self.columns_list=list(self.columns_list)
        for ele in range(len(self.columns)):
            if self.columns[ele] == column_name:
                i = ele       
        self.columns_list.remove(self.columns[i])
        self.columns=list(self.columns_list)
        for item in range(len(self.data)):
            self.data[item].remove(self.data[item][i])
        index_dict = {'yes_flag':list(self.index.keys())}
        self.data.append(index_dict)
        return DataFrame(self.data,self.columns)
      
        
    def as_type(self, column, cast_type):
        self.list_data[column] = list(map(cast_type, self.list_data[column]))
        for ele in range(len(self.columns)):
            if self.columns[ele] == column:
                i = ele
        for ele in range(len(self.data)):
            for item in range(len(self.data[ele])):
                if item == i:                  
                    try:                      
                        self.data[ele][item]=cast_type(self.data[ele][item])
                    except:
                        self.data[ele][item]=self.data[ele][item]
        return DataFrame(self.data,self.columns)

    
    def mean(self):
        return {self.columns[ele]:[ListV2([self.data[ele2][ele] for ele2 in range(len(self.data))]).mean() for ele in range(len(self.columns))][ele] for ele in range(len(self.columns))}

    def loc(self,index):
        if isinstance(index, int):
            return list({key: value[index] for key, value in self.list_data.items()}.values())[1:]
        elif isinstance(index, slice):
            return list({key: value[index.start:index.stop:index.step] for key, value in self.list_data.items()}.values())[1:]
        elif isinstance(index, tuple):
            col = index[1]
            rows=index[0]
            row_index_list=[]
            col_index_list=[]
            selected_rows=[]
            for ele in rows:
                for index_number,index_ele in enumerate(list(self.index.keys())):
                    if ele == index_ele:
                        row_index_list.append(index_number)
            for ele in col:
                for i,column in enumerate(self.columns):
                    if ele == column:
                        col_index_list.append(i)
            for i,ele in enumerate(self.data):
                if i in row_index_list:                 
                    selected_rows.append([ele[i] for i in col_index_list])
            self.columns=col
            self.data=selected_rows
            index_list={keys:value for keys, value in self.index.items() if value in row_index_list}
            index_dict = {'yes_flag':list(index_list.keys())}
            selected_rows.append(index_dict)
            return DataFrame(selected_rows,col)
        
    def iterrows(self):
        rows_tuple=[]
        final_list=[]
        for ele in list(self.index.keys()):
            rows_tuple.append([ele])
        print(rows_tuple)
        for i,ele in enumerate(self.data):
            rows_tuple[i].append(tuple(ele))
        final_list = [tuple(ele) for ele in rows_tuple]
        return final_list
            

         
            
