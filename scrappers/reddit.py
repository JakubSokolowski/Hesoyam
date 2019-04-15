import praw
from praw.models.reddit.submission import Submission
from praw.models.reddit.more import MoreComments
import credsmanager as m
from pathlib import Path
import csv
import json
import os
import time
import datetime
from collections import defaultdict

submission_header = [
    "id",
    "created_utc",
    "title",
    "selftext",
    "score",
    "upvote_ratio",
    "permalink",
    "num_comments",
    "comments"
]


class RedditScrapper:
    def __init__(self):
        self.creds = m.get_credentials('reddit')
        self.config = m.get_config('reddit')
        self.reddit = None
        self.data_path = str(Path(__file__).parents[1]) + '/data/'
        self.connected = False
        self.silent = True
        return

    def connect(self) -> None:
        """
        Connects to reddit using credentials, and returns praw.Reddit instance
        :return:
        """
        self.reddit = praw.Reddit(
            client_id=self.creds['client_id'],
            client_secret=self.creds['client_secret'],
            password=self.creds['password'],
            user_agent=self.creds['user_agent'],
            username=self.creds['username']
        )
        print("Connected as: ", self.reddit.user.me())
        self.connected = True

    def create_dirs(self):
        """
        Creates all the directories for subs specified in config
        :return:
        """
        for sub in self.config['subreddits']:
            path = self.data_path + sub['name']
            if not os.path.exists(path):
                os.makedirs(path)
        return

    def create_sub_files(self):
        """
        Creates all the submission and blacklist files for each directory
        :return:
        """
        for sub in self.config['subreddits']:
            name = sub['name']
            submissions_file = self.data_path + name + "/" + name + "_submissions.csv"
            blacklist_file = self.data_path + name + "/" + name + "_blacklist.csv"
            if not os.path.isfile(blacklist_file):
                file = open(blacklist_file, 'a+')
                writer = csv.writer(file, delimiter='|')
                writer.writerow(["submission_id"])
            if not os.path.isfile(submissions_file):
                file = open(submissions_file, 'a+')
                writer = csv.writer(file, delimiter='|')
                writer.writerow(submission_header)
            open(blacklist_file, 'a+')

    def process_top(self, submission_id: str):
        submission = self.reddit.submission(submission_id)
        submission.comment_sort = 'top'
        top = submission.comments.list()[:5]
        top_comments = []
        for comment in top:
            if isinstance(comment, MoreComments):
                continue
            comm_dict = {}
            comm_dict['created'] = comment.created
            comm_dict['score'] = comment.score
            comm_dict['body'] = comment.body
            comm_dict['replies'] = len(comment.replies)
            top_comments.append(comm_dict)
        return top_comments

    def process_controversial(self, submission_id: str):
        submission = self.reddit.submission(submission_id)
        submission.comment_sort = 'controversial'
        controversial = submission.comments.list()[:5]
        controversial_comments = []
        for comment in controversial:
            if isinstance(comment, MoreComments):
                continue
            comm_dict = {}
            comm_dict['created'] = comment.created
            comm_dict['score'] = comment.score
            comm_dict['body'] = comment.body
            comm_dict['replies'] = len(comment.replies)
            controversial_comments.append(comm_dict)
        return controversial_comments

    def process_comments(self, submission_id: str):
        top = self.process_top(submission_id)
        controversial = self.process_controversial(submission_id)
        comments = defaultdict()
        comments['top'] = top
        comments['controversial'] = controversial
        return json.dumps(comments)

    def process_submission(self, sub_name: str, submission: Submission):
        return [
            submission.id,
            submission.created_utc,
            submission.title.replace('|', ' '),
            submission.selftext.replace('|', ' '),
            submission.score,
            submission.upvote_ratio,
            submission.permalink,
            submission.num_comments,
            self.process_comments(submission.id).replace('|', ' ')
        ]

    def test(self):
        submission = self.reddit.submission(id='5or86n')
        self.process_comments(submission)

    def load_blacklist(self, sub_name: str):
        """
        Loads all the submission ids from sub specified by name, that have already been scrapped.
        Returns the set of blacklisted submissions
        :param sub_name:
        :return: set containing all submission_ids of blacklisted posts
        """
        path = self.data_path + sub_name + "/" + sub_name + "_blacklist.csv"
        posts = set()
        try:
            file = open(path, 'r')
        except IOError:
            file = open(path, 'w')
            return posts
        reader = csv.reader(file)
        for post in reader:
            posts.add(post[0])
        return posts

    def update_blacklist(self, sub_name: str, submission_ids: []):
        """
        Appends new submision ids to blacklist specified by sub_name
        :param sub_name:
        :param submission_ids:
        :return:
        """
        with open(self.data_path + sub_name + "/" + sub_name + "_blacklist.csv", 'a') as bl:
            writer = csv.writer(bl)
            for row in submission_ids:
                writer.writerow([row])
        return

    def update_submissions(self, sub_name: str, rows: []):
        """
        Writes submissions data in rows[] list to submission file specified by sub_name
        :param sub_name:
        :param rows:
        :return:
        """
        path = self.data_path + sub_name + "/" + sub_name + "_submissions.csv"
        with open(path, 'a') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerows(rows)

    def scrap_sub(self, sub_name: str):
        """
        Gets all the available submissions from subreddit specified by sub_name
        and saves them to submission file
        :param sub_name:
        :return:
        """
        if not self.silent:
            print("Starting scrapping sub: ", sub_name)
        scraping_start = time.time()
        blacklist = self.load_blacklist(sub_name)
        saved_submission_ids = []
        saved_submissions = []
        for submission in self.reddit.subreddit(sub_name).hot(limit=1000):
            if submission.id not in blacklist:
                start = time.time()
                row = self.process_submission(sub_name, submission)

                saved_submission_ids.append(submission.id)
                saved_submissions.append(row)
                end = time.time()
                if not self.silent:
                    message = "Scrapped submission: " \
                              + "ID: " + row[0] \
                              + " Title: " + self.format_title(row[2]) \
                              + " in " + str(end - start)
                    print(message)
        self.update_submissions(sub_name, saved_submissions)
        self.update_blacklist(sub_name, saved_submission_ids)
        scraping_end = time.time()
        if not self.silent:
            message = "Scraped /r/" + sub_name + " in " + str(scraping_end - scraping_start)
            print(message)
        return

    def start(self, silent: bool):
        """
        Starts scraping subreddits
        :param silent:
        :return:
        """
        self.silent = silent
        self.create_dirs()
        start = time.time()
        for sub in self.config['subreddits']:
            self.scrap_sub(sub['name'])
        end = time.time()
        if not self.silent:
            message = "Finished scrapping subs in : " + str(datetime.timedelta(seconds=(end - start)))
            print(message)
        return

    @staticmethod
    def format_title(title: str):
        if len(title) > 70:
            return title[:64] + "\t [...]"
        else:
            return title + ' ' * (70 - 6 - len(title)) + "\t [...]"

    def get_sub_data_path(self, sub_name: str):
        return self.data_path + sub_name + '/' + sub_name + "_submissions" + '.csv'
