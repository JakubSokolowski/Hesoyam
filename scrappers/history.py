import csv
import urllib.request
import json
from urllib.error import HTTPError
from pymongo import MongoClient
import credsmanager as m
import time
import datetime
import pymongo
from db import dbmanager as dbm
from datetime import datetime as dt
from pathlib import Path

query_template = "https://api.pushshift.io/reddit/search/submission/?" \
                 "subreddit={}&" \
                 "after={}&" \
                 "sort=asc&limit={}"


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


class HistoricalRedditScrapper:
    def __init__(self):
        self.config = m.get_config('reddit')
        self.data_path = str(Path(__file__).parents[1]) + '/data_history/'
        self.start_date = '1451606400'
        self.silent = False
        self.db = dbm.DataBaseManager()

    @staticmethod
    def make_request(query: str):
        """
        Sends query to pushift.io api
        :param query:
        :return:
        """
        with urllib.request.urlopen(query) as url:
            return json.loads(url.read().decode())

    def scrap_all(self):
        for sub in self.config['subreddits']:
            self.scrap_sub(sub['name'])

    def scrap_sub(self, sub_name: str):
        index = self.get_sub_index(sub_name)
        if self.sub_first_scrap(index):
            self.update_after_date(index, self.start_date)
        if not self.silent:
            print("Starting to scrap : {}".format(sub_name))
        start = time.time()
        while True:
            date = self.get_after_date(index)
            scrapped_data = self.scrap_sub_after_date(index, date)
            if len(scrapped_data) == 0:
                break
            new_after_date = scrapped_data[-1]['created_utc']
            self.update_submissions(scrapped_data, sub_name)
            self.update_after_date(index, new_after_date)
            m.update_config(self.config, 'reddit')
        end = time.time()
        timestamp = str(datetime.timedelta(seconds=(end - start)))
        if not self.silent:
            print("Finished scrapping {} in {}".format(sub_name, timestamp))

    def scrap_sub_after_date(self, index: int, date: str, limit: int = 1000):
        """
        Scraps all the submissions from subreddit specified by sub_name that
        were made after specified date
        :param index:
        :param limit:
        :param date: date string in UTC format
        :return: json with submissions
        """
        start = time.time()
        sub_name = self.config['subreddits'][index]['name']
        query = query_template.format(sub_name, date, limit)
        data = HistoricalRedditScrapper.make_request(query)['data']
        end = time.time()
        timestamp = str(datetime.timedelta(seconds=(end - start)))
        if len(data) > 0:
            self.print_summary(data, timestamp)
        return data

    def sub_first_scrap(self, index: int):
        return 'currentAfterDate' not in self.config['subreddits'][index]

    def update_after_date(self, index: int, new_after_date: str):
        self.config['subreddits'][index]['currentAfterDate'] = new_after_date

    def get_after_date(self, index: int):
        return self.config['subreddits'][index]['currentAfterDate']

    def get_sub_index(self, sub_name: str):
        for index, item in enumerate(self.config['subreddits']):
            if item['name'] == sub_name:
                break
        else:
            index = -1
        return index

    def print_summary(self, scrapped_data, time_elapsed):
        if self.silent:
            return
        size = len(scrapped_data)
        from_date = dt.utcfromtimestamp(scrapped_data[0]['created_utc'])
        to_date = dt.utcfromtimestamp(scrapped_data[-1]['created_utc'])
        print("Scrapped {} submissions from {} to {} in {}".
              format(size, from_date, to_date, time_elapsed))

    def update_submissions(self, scrapped_data, sub_name: str):
        self.db.update_db('reddit', sub_name + "_history", scrapped_data)
        # database = self.mongo.reddit
        # collection = database[sub_name + "_history"]
        # collection.insert_many(scrapped_data)

    def get_comments(self, submission_id: str):
        ids = self.get_comment_ids(submission_id)
        if len(ids) == 0:
            return {}
        print("Submission {} Num comments {}".format(submission_id, len(ids)))
        query = "https://api.pushshift.io/reddit/comment/search?ids="
        data = []
        try:
            for chunk in chunks(self.get_comment_ids(submission_id), 1000):
                print("\tProcessing chunk of size {}".format(len(chunk)))
                query = query + ','.join(chunk)
                data = data + HistoricalRedditScrapper.make_request(query)['data']
        except HTTPError:
            return data
        return data

    def get_comment_ids(self, submission_id: str):
        query = "https://api.pushshift.io/reddit/submission/comment_ids/" + submission_id
        return HistoricalRedditScrapper.make_request(query)['data']

    def get_all_comments(self):
        client = MongoClient("mongodb://localhost:27017/")
        db = client.reddit
        for sub in self.config['subreddits']:
            collection = db[sub['name'] + '_history']
            print("Starting getting comments for sub " + sub['name'])
            for post in collection.find({}, no_cursor_timeout=True):
                print("Searching for comments for post {} from sub {}".format(post['id'], sub['name']))
                if post['num_comments'] < 6:
                    post['comments'] = []
                    post['comments_scrapped'] = 1
                    collection.save(post)
                    continue
                if 'comments' not in post:
                    comments = self.get_comments(post['id'])
                    post['comments'] = comments
                    post['comments_scrapped'] = 1
                    collection.save(post)
                else:
                    if post['comments_scrapped'] == 0:
                        comments = self.get_comments(post['id'])
                        post['comments'] = comments
                        post['comments_scrapped'] = 1
                        collection.save(post)


scrapper = HistoricalRedditScrapper()
# print(scrapper.get_comment_ids_as_str("6xjaba"))
# print(scrapper.get_comments("6xjaba"))
scrapper.get_all_comments()
# print(len(scrapper.get_comments("4oiqj7")))
# db.ArkEcosystem_history.find( { id: { $eq: "570e0o" } } )
