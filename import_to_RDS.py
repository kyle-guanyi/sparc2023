import requests
import json
import mysql.connector

# code for popular TV_series
def run():
    number_of_pages = getTotalPages()

    conn, cursor = sqlConnInit()
    for i in range(1, number_of_pages+1):
        records_to_insert = apiRecordPerPage(i)
        insertPage(cursor, records_to_insert)
        conn.commit()
        print(f"page{i} successfully insert to database")

    # Close the cursor and connection# Commit the changes
    cursor.close()
    conn.close()


def getTotalPages():
    url = "https://api.themoviedb.org/3/tv/popular"
    headers = {
        "accept": "application/json",
        "Authorization": ""
    }
    response = requests.get(url, headers=headers)
    response_dict = response.json()
    number_of_pages = response_dict['total_pages']
    return number_of_pages


def apiRecordPerPage(page_number):
    url = "https://api.themoviedb.org/3/tv/popular"
    headers = {
        "accept": "application/json",
        "Authorization": ""
    }
    response = requests.get(url, params={'page': page_number}, headers=headers)

    response.status_code

    response_dict = response.json()

    array_of_result = response_dict['results']
    records_to_insert = []
    # remove genre_ids and origin_country from json object
    for i in range(len(array_of_result)):
        del array_of_result[i]["genre_ids"]
        del array_of_result[i]["origin_country"]
        obj = array_of_result[i]
        backdrop_path = obj.get("backdrop_path")
        first_air_date = obj.get("first_air_date")
        id = obj.get("id")
        name = obj.get("name")
        original_language = obj.get("original_language")
        original_name = obj.get("original_name")
        overview = obj.get("overview")
        popularity = obj.get("popularity")
        poster_path = obj.get("poster_path")
        vote_average = obj.get("vote_average")
        vote_count = obj.get("vote_count")
        row_value = (
            backdrop_path, first_air_date, id, name, original_language, original_name, overview, popularity, poster_path,
            vote_average, vote_count)

        records_to_insert.append(row_value)
    return records_to_insert


def insertPage(cursor, records_to_insert):
    try:

        # Modify the SQL query to insert the values into the desired table
        insert_query = "INSERT INTO TV_SERIES_POPULAR_LIST (backdrop_path, first_air_date, id, name, original_language, original_name, overview, popularity, poster_path, vote_average, vote_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # Execute the SQL query with the extracted values
        cursor.executemany(insert_query, records_to_insert)

    except KeyError as e:
        print(f"Error: Required key '{str(e)}' is missing in the object.")
        # Handle the error in an appropriate way, such as logging or error handling logic


def sqlConnInit():
    # Define the necessary database connection details
    host = 'sparc-db.c56gikbu12n4.us-east-1.rds.amazonaws.com'
    port = '3306'
    database = ''
    user = ''
    password = ''
    # Establish a connection to the RDS instance
    conn = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password)
    # Create a cursor object
    cursor = conn.cursor()
    return conn, cursor

# run the app
run()
