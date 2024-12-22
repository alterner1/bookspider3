# bookspider
Scrapy Tutorial - An open source and collaborative framework for extracting the data you need from websites.

Visit [scrapy.org](https://scrapy.org/) for more information.

# How to install Scrapy (Ubuntu 20.04.5 LTS)

## Create Python virtual environments
```
sudo apt install python3.12-venv
python3 -m venv venv
source venv/bin/acrivate
```

## Install Scrapy
```
pip install scrapy
```

## Create your Scrapy project
```
scrapy startproject bookvoed
```

## Build and run your first spider with:
```
cd bookvoed
scrapy genspider bookstore bookstore.com
scrapy crawl bookspider
```

## Spiders: 
Spider 'urls2queue' fills the Redis queue with urls. Spider 'bookspider' reads urls from the queue and parses pages at those links.
It can be run in a distributed mode, with multiple instances running simultaneously.

```
scrapy crawl urls2queue
scrapy crawl bookspider
```
