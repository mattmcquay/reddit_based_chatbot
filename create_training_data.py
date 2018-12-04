import sqlite3
import pandas as pd

def create_training_data(database_location, limit, file_location, test_done=False):
    """
    Parameters:
        - database_location: the sqlite file location, used to connect to the database
        - limit: the maximum number of rows buffered at once
        - file_location: the directory to store all the data files
        - test_done: will either be True or False, this will determine whether or not the "test" data has been created
    """
    ## Setup the connection, create a cursor and initialise the counter, cur_length, last_unix time and test_done status
    connection = sqlite3.connect(database_location)
    c = connection.cursor()

    counter = 0
    cur_length = limit
    last_unix = 0
    test_done = test_done
    

    ## Start iterating through the data
    while cur_length == limit:
        ## The data frame is created by taking the next limit length of rows from the database
        ## The data is pulled back in chronological order, with the latest unix timestamp being used to determine where the query should start from
        ## When the cur_length is not equal to the limit we know we are on the last iteration
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} AND parent NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix, limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)

        ## A set of test data will be created the size of the limit parameter
        if not test_done:
            with open(file_location + "/test.from","a", encoding="utf8") as f:
                for content in df['parent'].values:
                    f.write(content + '\n')
            with open(file_location + "/test.to", "a", encoding="utf8") as f:
                for content in df['comment'].values:
                    f.write(content + '\n')
            ## Once this has occurred once the test will be set to complete
            test_done = True
        
        ## Once the test data is created the training data can be formed
        else:
            with open(file_location + "/train.from","a", encoding="utf8") as f:
                for content in df['parent'].values:
                    f.write(content + '\n')
            with open(file_location + "/train.to", "a", encoding="utf8") as f:
                for content in df['comment'].values:
                    f.write(content + '\n')
        
        ## Increment the counter every time it iterates, then print every 20 loops
        counter += 1
        if counter % 20 == 0:
            print(counter*limit, "rows completed so far")


if __name__ == "__main__":
    create_training_data('C:/Users/MMCQUAY/Documents/Reddit Data/reddit_data.db', 5000, 'C:/Users/MMCQUAY/Documents/Reddit Data', False)