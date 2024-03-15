import psycopg2
import psycopg2.extras

hostname = 'localhost'
database = 'dvdrental'
username = 'postgres'
pwd = 'password'
port_id = 5432
conn = None

try:
    with psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id) as conn:

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
    
            # droping table
            cur.execute('DROP TABLE IF EXISTS employee')

            # creating table
            create_script = '''CREATE TABLE IF NOT EXISTS employee (
                                id      int PRIMARY KEY,
                                name    varchar(40) NOT NULL,
                                salary  int,
                                dept_id varchar(30))'''
            cur.execute(create_script)

            # inserting data
            insert_script = '''INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s, %s, %s)'''
            insert_values = [(1, 'james', 12000, 'D1'), (2, 'robin', 15000, 'D1'), (3, 'xavier', 12000, 'D2')]
            for record in insert_values:
                cur.execute(insert_script, record)

            # updating table
            update_script = 'UPDATE employee SET salary = salary + (salary*0.5)'
            cur.execute(update_script)

            # deleting data
            delete_script = 'DELETE FROM employee WHERE name = %s'
            delete_record = ('james',)
            cur.execute(delete_script, delete_record)

            # fetching data
            cur.execute('SELECT * FROM employee')
            for record in cur.fetchall():
                print(record['name'], record['salary'])

except Exception as error:
    print(error)

finally:
    if conn is not None:
        conn.close()
    