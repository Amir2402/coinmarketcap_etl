import RetApi 
import pandas as pd 

def transform_data() :
    my_data = RetApi.get_json_data() 
    my_dataframe = pd.json_normalize(my_data['data'])
    pd.set_option('display.max_columns', None)
    my_dataframe.dropna(axis = 1 , inplace = True )
    new_cols = []

    for column_name in list(my_dataframe.columns) : 

        if 'quote' in column_name: 
            column_name = column_name.split('.')[2]
            new_cols.append(column_name)

        else : 
            new_cols.append(column_name)

    my_dataframe.columns = new_cols
    needed_data = my_dataframe[['id' , 'name' , 'symbol' , 'price']]
    needed_data.set_index('id' , inplace = True )
    needed_data.to_csv('/home/amir/firstproj/extracted_data.csv')
    
if __name__ == '__main__' : 
    transform_data() 