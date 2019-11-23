import sys
from flask import escape
import requests
import bs4 as bs
from urllib import parse
import json
from google.cloud import pubsub_v1

def publish(messages):
    project_id = "pol-pipe"
    topic_name = "phub-url"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    for message in messages:
        future = publisher.publish(
            topic_path, data=message.encode('utf-8')
        )

def scrape_urls(request):
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'pages' in request_json:
        pages = int(request_json['pages'])
    elif request_args and 'pages' in request_args:
        pages = int(request_args['pages'])
    else:
        pages = 3

    urls = []
    for i in range(1,pages,1):
        url = 'https://www.pornhub.com/video?o=mv&t=a&page=' + str(i)
        listPage = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'})
        parsedListPage = bs.BeautifulSoup(listPage.text, 'html.parser')

        dom_urls = parsedListPage.select('.img.fade.fadeUp.videoPreviewBg a')
        dom_urls = ['https://www.pornhub.com' + tag['href'] for tag in dom_urls]
        publish(dom_urls)
    return ('Scraped all URLs.')

if __name__ == '__main__':
    from flask import Flask, request
    app = Flask(__name__)

    # option 1
    @app.route('/', methods=['POST', 'GET'])
    def test():
        return scrape_urls(request)

    # option 2
    app.add_url_rule('/scrape_urls', 'scrape_urls', scrape_urls, methods=['POST', 'GET'], defaults={'request': request})

    app.run(host='127.0.0.1', port=8088, debug=True)