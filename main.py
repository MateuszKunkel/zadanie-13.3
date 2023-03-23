import sqlite3
import csv
from sqlite3 import Error
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, MetaData


# LAUNCH_________________________________________


def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def create_connection_in_memory():
    conn = None
    try:
        conn = sqlite3.connect(":memory:")
        print(f"Połączono na sqlite ver. {sqlite3.version}")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Połączono z plikiem {db_file}, sqlite ver. {sqlite3.version}")
        return conn
    except Error as e:
        print(e)
    return conn


# ADD_FUNCTIONS_/_ADD_CSV________________________


def add_station(conn, station):
    sql = """INSERT INTO stations(station, latitude, longitude, elevation, name, country, state)
        VALUES (?,?,?,?,?,?,?)"""
    c = conn.cursor()
    c.execute(sql, station)
    conn.commit()
    return c.lastrowid


def add_measure(conn, measure):
    sql = """INSERT INTO measures(station, date, precib, tobs)
        VALUES (?,?,?,?)"""
    c = conn.cursor()
    c.execute(sql, measure)
    conn.commit()
    return c.lastrowid


def add_station_csv(ins_station):
    with open("clean_stations.csv", "r") as f:
        csvr = csv.reader(f, delimiter=",")
        next(csvr)
        engine.execute(
            ins_station,
            [
                {
                    "station": row[0],
                    "latitude": row[1],
                    "longitude": row[2],
                    "elevation": row[3],
                    "name": row[4],
                    "country": row[5],
                    "state": row[6],
                }
                for row in csvr
            ],
        )


def add_measure_csv(ins_measure):
    with open("clean_measure.csv", "r") as f:
        csvr = csv.reader(f, delimiter=",")
        next(csvr)
        engine.execute(
            ins_measure,
            [
                {"station": row[0], "date": row[1], "precib": row[2], "tobs": row[3]}
                for row in csvr
            ],
        )


# UPDATE_FUNCTIONS__________________________________


def update(conn, table, id, **kwargs):
    """
    :param kwargs: has to be dict of attributes and values
    """
    par = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(par)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
            SET {parameters}
            WHERE id = ?"""
    try:
        c = conn.cursor()
        c.execute(sql, values)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(e)


# SELECT_FUNCTIONS__________________________________


def select_where(conn, table, **query):
    """
    :param query: has to be dict of attributes and values
    :return: all rows
    """
    c = conn.cursor()
    qs = []
    val = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        val += (v,)
    q = " AND ".join(qs)
    c.execute(f"SELECT * FROM {table} WHERE {q}", val)
    rows = c.fetchall()
    return rows


def select_all(conn, table):
    """
    :return: all rows
    """
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table}")
    rows = c.fetchall()
    return rows


# DELETE_FUNCTIONS__________________________________


def delete_where(conn, table, **kwargs):
    """
    :param kwargs: has to be dict of attributes and values
    """
    qs = []
    val = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        val += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    c = conn.cursor()
    c.execute(sql, val)
    conn.commit()
    print("DELETED")


def delete_all(conn, table):
    sql = f"DELETE FROM {table}"
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    print("DELETED ALL")


# MAIN_FUNCTIONS______________________________________


if __name__ == "__main__":
    db_file = "stations-13.3.db"
    conn = create_connection(db_file)
    engine = create_engine("sqlite:///stations-13.3.db", echo=True)

    meta = MetaData()

    all_stations = Table(
        "stations",
        meta,
        Column("station", String, primary_key=True),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("name", String),
        Column("country", String),
        Column("state", String),
    )

    all_measures = Table(
        "measures",
        meta,
        Column("station", String),
        Column("date", String),
        Column("precib", Float),
        Column("tobs", Integer),
    )

    meta.create_all(engine)

    create_stations_sql = """
    CREATE TABLE IF NOT EXISTS stations  (
        id integer PRIMARY KEY,
        station text NOT NULL,
        latitude float NOT NULL,
        longitude float NOT NULL,
        elevation float NOT NULL,
        name text NOT NULL,
        country text NOT NULL,
        state text NOT NULL
    );
    """

    create_measures_sql = """
    CREATE TABLE IF NOT EXISTS measures  (
        id integer NOT NULL,
        station text NOT NULL,
        date text NOT NULL,
        precib float NOT NULL,
        tobs integer NOT NULL
    );
    """

    # _________activate to create full database from csv files
    # v
    #ins_stations = all_stations.insert()
    #ins_measures = all_measures.insert()
    #add_station_csv(ins_stations)
    #add_measure_csv(ins_measures)
    # ^

    # _________activate to add new station/measure, write correct variable into tuples
    # v
    # station = ('station', 'latitude', 'longitude', 'elevation', 'name', 'country', 'state')
    # station_id = add_station(conn, station)
    # measure = ('station', 'date', 'precib', 'tobs')
    # measure_id = add_measure(conn, measure)
    # ^

    # _________activate to check if "select" and "delete" commands work
    # v
    #print(select_all(conn, "stations"))
    #delete_where(conn, "stations", station="USC00516128")
    #print(select_all(conn, "stations"))
    #print(select_where(conn, "measures", station="USC00519397"))
    #delete_where(conn, "measures", precib=0.0)
    #print(select_where(conn, "measures", station="USC00519397"))
    # ^
