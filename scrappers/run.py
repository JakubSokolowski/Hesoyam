import scrappers.reddit


scrapper = scrappers.reddit.RedditScrapper()
scrapper.connect()
scrapper.create_dirs()
scrapper.create_sub_files()

scrapper.start(silent=False)
