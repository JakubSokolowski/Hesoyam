from scrappers.reddit import RedditScrapper


scrapper = RedditScrapper()
scrapper.connect()
scrapper.create_dirs()
scrapper.create_sub_files()
scrapper.start(silent=False)
