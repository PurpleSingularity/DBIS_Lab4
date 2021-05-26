from config import Connect,NamePath_files
from bson.son import SON
from time_wraper import profile_time
import csv
import time

step = 10000
query = """[
            {
                "$match" : { 
                                "physTestStatus" :  'Зараховано' 
                            }
            },
            {
                "$group" : { 
                                "_id" : {
                                            "region" : "$REGNAME",
                                            "year" : "$year",
                                        },
                                "PhysAvgResults": { "$avg": "$physBall100" }
                            }
            },
            {
                "$sort" : { "_id.region": 1, "_id.year": 1 }
            }]"""



def create_collection():
    t0 = time.perf_counter() #time counter ------------------------------------------------------
    connection = Connect.get_connection()
    db = connection['Lab4']
    # db.drop_collection('ZNO_result')
    collection = db['ZNO_result']
    print("In DATABASE: - ", collection.count_documents({}))
    print('============================================')
    t1 = time.perf_counter() #time counter ------------------------------------------------------
    with open('result_time.txt',"w") as ftime:
        ftime.write("DATABASE connection: ")
        ftime.write(str(t1-t0)+"s\n")
    return collection

def insert_into_database(collection):
    t0 = time.perf_counter() #time counter ------------------------------------------------------
    for year in [2019,2020]:
        print(str(year) + ' file')
        print("-"*100)
        max_row_number = step
        with open(NamePath_files.format(year=year), encoding='cp1251') as f:
            header = []
            HEADER_CREATING_FLG = False
            data = csv.reader(f,delimiter=';',quotechar='"',quoting=csv.QUOTE_ALL)   
            i = 1            
            IN_DB_ROWS = collection.count_documents({"year" : year})
            if IN_DB_ROWS:
                max_row_number = IN_DB_ROWS + step
            
            for row in data:
                if IN_DB_ROWS < i and HEADER_CREATING_FLG:
                    row.append(year)
                    i, max_row_number = line_to_dict_list(header, row, i, step, max_row_number,year, collection)
                else:
                    if IN_DB_ROWS == i:
                        print(i, 'Data is already in DATABASE')
                        print("------------------------------------------------------------------------------")
                    i += 1
                if header == []:
                    header = row
                    header.append("year")
                    HEADER_CREATING_FLG = True
        print('End {yearFile} file to DB\nIn Collection {lines} lines'.format(yearFile = year,lines = collection.count_documents({})))
        print('============================================')
    t1 = time.perf_counter() #time counter ------------------------------------------------------
    with open('result_time.txt',"a") as ftime:
        ftime.write("Insert: ")
        ftime.write(str(t1-t0)+"s\n")

def clean_csv(value):
    if value == 'null':
        return value
    try:
        res = float(value.replace(',', '.'))
        return res
    except:
        return value

def line_to_dict_list(header, row, i, step, max_row_number,year, collection):
    i += 1
    line_dict = { h_value : "-" for h_value in header }
    for j in range(len(header)):
        if clean_csv(row[j]) == row[j]:
            line_dict[header[j]] = row[j]
        else:
            line_dict[header[j]] = clean_csv(row[j])
    collection.insert_one(line_dict)
        
    if max_row_number < i:
        print(max_row_number, "------------------------------------------------------------------------------")
        max_row_number += step
        

    return i, max_row_number

    
def execute_query(collection):
    header = ["Region", "Year", "Avg Phys Results"]
    
    with open('result_query.csv', 'w', newline = '') as res_file:
        t0 = time.perf_counter() #time counter ------------------------------------------------------
        result_writer = csv.writer(res_file, delimiter=';')
        result_writer.writerow(header)
        result = collection.aggregate([
            {
                "$match" : { 
                                "physTestStatus" :  'Зараховано' 
                            }
            },
            {
                "$group" : { 
                                "_id" : {
                                            "region" : "$REGNAME",
                                            "year" : "$year",
                                        },
                                "PhysAvgResults": { "$avg": "$physBall100" }
                            }
            },
            {
                "$sort" : { "_id.region": 1, "_id.year": 1 }
            }])
        lst = []
        for element in result:
            lst.append([element["_id"]["region"], element["_id"]["year"], element["PhysAvgResults"]])
        result_writer.writerows(lst)
    t1 = time.perf_counter() #time counter ------------------------------------------------------
    with open('result_time.txt',"a") as ftime:
        ftime.write("Query: ")
        ftime.write(str(t1-t0)+"s\n")
        

if __name__ == '__main__':
    collection = create_collection()
    
    insert_into_database(collection)
    
    execute_query(collection)