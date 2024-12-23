# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

# Saving Scraped Data To MySQL Database With Scrapy Pipelines
# based on this tutorial https://scrapeops.io/python-scrapy-playbook/scrapy-save-data-mysql/

from itemadapter import ItemAdapter
import os
import psycopg2
from psycopg2 import sql


class BookvoedPipeline:

    def __init__(self):
        #self.conn = psycopg2.connect(os.getenv('CONNECTION_STRING'))
        db_name = os.getenv('db_name')
        db_user = os.getenv('db_user')
        db_passwd = os.getenv('db_passwd')
        db_host = os.getenv('db_host')
        self.conn = psycopg2.connect(host=db_host, port=6432, database=db_name, user=db_user,password=db_passwd )


        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()
        ## Create books table if none exists
        ## The database 'bookspider' must first be created
        ## mysql> CREATE DATABASE bookspider;
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS book_data (
                id serial PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                author VARCHAR(255),
                price VARCHAR(255) 
        )
        """)

    def process_item(self, item, spider):

        ## Check to see if book is already in database 
        self.cur.execute("SELECT * FROM books WHERE name = %s", (item['name'],))
        result = self.cur.fetchone()

        ## If it is in DB, create log message
        if result:
            spider.logger.warn("Item already in database: %s" % item['name'])

        ## If text isn't in the DB, insert data
        else:

            ## Define insert statement
            self.cur.execute("""INSERT INTO books (name, price, author) VALUES (%s, %s, %s)""", (
                item["name"],
                item["price"],
                item["author"]
            ))

            ## Execute insert of data into database
            self.conn.commit()
        return item

    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()