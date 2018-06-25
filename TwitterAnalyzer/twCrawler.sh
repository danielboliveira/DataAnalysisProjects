#!/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
cd "$(dirname "$0")";
python3 TwitterCrawlerToFiles.py crawler.config




