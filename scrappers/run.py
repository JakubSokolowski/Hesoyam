#!/usr/bin/python
from .reddit import RedditScrapper

scrapper = RedditScrapper()
scrapper.connect()
scrapper.start(silent=True)
