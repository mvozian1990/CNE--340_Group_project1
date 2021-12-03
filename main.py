
import matplotlib.pyplot as plot
import pandas as pd
from sqlalchemy import create_engine

db_user = 'root'
db_pwd = 'root'
db_host = 'localhost'
db_port = '8889'
db_name = 'CNE_340'
sql = "select statename, avg(ICUBedsOccAnyPat__N_ICUBeds_Est) as avg_occupied from covid19 group by statename order by avg_occupied"


def connect_database():
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(db_user, db_pwd, db_host, db_port, db_name))
    connect = engine.connect()
    return connect


def read_from_file(filename):
    df = pd.read_csv(filename, skiprows=[1])
    return df


def write_data_to_database(df, connect):
    df.to_sql(name='covid19', con=connect, if_exists='replace', index=False, index_label='id')


def read_data_from_database(connect):
    return pd.read_sql(sql, connect)


def plot_data(df):
    df.plot.barh(x='statename', y='avg_occupied', title='Average ICU Beds Occupancy By State (percent)')
    plot.show(block=True)


def main():
    # Read data from the CSV file into a data frame (using pandas)
    df = read_from_file('covid19-NatEst.csv')

    # Connect to our local MySql database
    connect = connect_database()

    # Create the table in the database and dump our data-frame data in it
    write_data_to_database(df, connect)

    # Read some data from the database
    df = read_data_from_database(connect)

    # Plot the data
    plot_data(df)


if __name__ == '__main__':
    main()
