import clickhouse_connect
import sys


def main():
    client = clickhouse_connect.get_client(host='localhost')
    
    # Create a table
    try:
        client.command('CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
    except Exception as e:
        print(e)

    # Insert data
    row1 = [1000, 'String Value 1000', 5.233]
    row2 = [2000, 'String Value 2000', -107.04]
    data = [row1, row2]
    client.insert('new_table', data, column_names=['key', 'value', 'metric'])

    # Select data
    ALL_ROWS_QUERY = "SELECT * FROM new_table"
    sys.stdout.write("query: ["+ALL_ROWS_QUERY + "] returns:\n\n")
    print(client.query(ALL_ROWS_QUERY).result_rows)

    QUERY = "SELECT max(key), avg(metric) FROM new_table"
    result = client.query(QUERY)

    sys.stdout.write("query: ["+QUERY + "] returns:\n\n")
    print(result.result_rows)

    # Drop table
    print("Dropping table new_table")
    client.command('DROP TABLE new_table')


if __name__ == '__main__':
    main()