import sqlite3
import json
from datetime import datetime

## Set the timeframe that we want to look at and data file location, then initialise the sql_transaction list and create a sql connection
timeframe = '2018-01'
file_location = "C:/Users/MMCQUAY/Documents/Reddit Data/RC_{}".format(timeframe)
sql_transaction = []

connection = sqlite3.connect('C:/Users/MMCQUAY/Documents/Reddit Data/reddit_data.db')
c = connection.cursor()

## Create the parent_reply table
def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")


## Sanitise the data sent, replacing \n and \r with " newlinechar " and double quotes with single quotes, to homogenise the data
def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data

## Finds the body of a comment from a parent_id
def find_parent(parent_id):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

## Finds the score of a comment linked to a parent_id
def find_existing_score(parent_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

## Returns True if the data is acceptable, or false if we do not want to consider it
def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True

## This function will take in sql and add statements to the sql_transaction list, then execute these in bulk
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

## The following 3 functions will create queries to replace a records data, insert a new record with an existing parent or insert a new record with no parent level
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion', str(e))
def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion', str(e))



if __name__ == "__main__":
    ## Create the parent_reply table and initialise counters
    create_table()
    row_counter = 0
    paired_rows = 0

    ## Open the data file and start parsing the data
    with open(file_location, buffering=1000) as f:
        for row in f:
            ## Increment the row_count and load the json data
            row_counter += 1
            row = json.loads(row)

            ## Split the json data into variables
            comment_id = row['id']
            parent_id = row['parent_id'].split('_')[1]
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            ## We perform operations to determine whether we want to consider the data
            ## We check score first, as this is very cheap to check
            ## We then check whether the data is acceptable to fit in the database, by calling the acceptable function which returns True or False
            ## Next we check whether the comment selected is the best comment - parent pair, as we only want the 'best' reply from a comment 

            if score >= 2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))