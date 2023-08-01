import psycopg2

class PostgresPipeline:
    def __init__(self, db_settings):
        self.db_settings = db_settings

    @classmethod
    def from_crawler(cls, crawler):
        # Get the PostgreSQL settings from Scrapy settings
        db_settings = crawler.settings.getdict('POSTGRES_SETTINGS')
        return cls(db_settings)

    def open_spider(self, spider):
        # Connect to the PostgreSQL database
        self.conn = psycopg2.connect(**self.db_settings)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        # Close the database connection when the spider is closed
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        # Insert the scraped data into the PostgreSQL database
        table_name = 'scrapy_data' #spider.settings.get('POSTGRES_TABLE_NAME', 'scrapy_data')  # Change 'scrapy_data' to your desired table name
        keys = item.keys()
        values = [item[k] for k in keys]

        try:
            # Create the INSERT query dynamically based on item keys
            insert_query = f"INSERT INTO {table_name} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(keys))})"
            self.cursor.execute(insert_query, values)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            spider.log(f"Error inserting data into PostgreSQL database: {e}")
        return item
