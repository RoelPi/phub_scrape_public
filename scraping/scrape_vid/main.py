import sys
from flask import escape
import requests
import bs4 as bs
from urllib import parse
import json
import base64

from google.cloud import pubsub_v1

def publish(messages, messageType, videoId):
    if messageType == 'video':
        project_id = "pol-pipe"
        topic_name = "phub-video"
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        for message in messages:
            data = {'videoId': videoId, 'url': str(message[0]), 'title': str(message[1]), 'op': str(message[2]), 'views': str(message[3])}
            data_json = json.dumps(data)
            data_json = data_json.encode('utf-8')
            future = publisher.publish(
                topic_path, data=data_json
            )
            print(future.result())
    
    if messageType == 'comment':
        project_id = "pol-pipe"
        topic_name = "phub-comment"
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        for message in messages:
            data = {'videoId': videoId, 'comment': str(message[0]), 'user': str(message[1])}
            data_json = json.dumps(data)
            data_json = data_json.encode('utf-8')
            future = publisher.publish(
                topic_path, data=data_json
            )

    if messageType == 'category':
        project_id = "pol-pipe"
        topic_name = "phub-category"
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        for message in messages:
            data = {'videoId': videoId, 'category':str(message)}
            data_json = json.dumps(data)
            data_json = data_json.encode('utf-8')
            future = publisher.publish(
                topic_path, data=data_json
            )
    
    if messageType == 'tag':
        project_id = "pol-pipe"
        topic_name = "phub-tag"
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        for message in messages:
            data = {'videoId': videoId, 'tag':str(message)}
            data_json = json.dumps(data)
            data_json = data_json.encode('utf-8')

            future = publisher.publish(
                topic_path, data=data_json
            )
    
def scrape_vid(event, context):
    if 'data' in event:
        url = base64.b64decode(event['data']).decode('utf-8')
    else:
        url = 'https://nl.pornhub.com/view_video.php?viewkey=ph5d38cc3d2fa9a'

    videoId = parse.parse_qs(parse.urlparse(url).query)['viewkey'][0]

    page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'})
    parsedPage = bs.BeautifulSoup(page.text, 'html.parser')

    # title
    dom_title = parsedPage.select_one('.video-wrapper .title-container h1.title .inlineFree')
    if dom_title is None:
        dom_title = 'error'
    else:
        dom_title = dom_title.text.strip()

    # Views
    dom_views = parsedPage.select_one('.rating-info-container > .views')
    if dom_views is None:
        dom_views = 'error'
    else:
        dom_views = int(dom_views.text.strip().replace(' views','').replace(',','').replace(' ',''))

    # OP
    dom_op = parsedPage.select_one('.video-info-row .usernameWrap .bolded')
    if dom_op is None:
        dom_op = 'error'
    else:
        dom_op = dom_op.text.strip()

    # Categories
    dom_categories = parsedPage.select('.categoriesWrapper a')
    dom_categories = [category.text.strip() for category in dom_categories]
    if '+ Suggest' in dom_categories:
        dom_categories.remove('+ Suggest')

    # Tags
    dom_tags = parsedPage.select('.tagsWrapper a')
    dom_tags = [tag.text.strip() for tag in dom_tags]
    if '+ Suggest' in dom_tags:
        dom_tags.remove('+ Suggest')

    # Comment messages
    dom_comments_messages = parsedPage.select('.commentMessage > span')
    dom_comments_messages = ['<br />'.join(message.text.strip().split('\n')) for message in dom_comments_messages]
    if len(dom_comments_messages) > 0:
        del dom_comments_messages[-1]

    # Comment users
    dom_comments_users = parsedPage.select('.commentBlock .boxUserComments .userLink img')
    dom_comments_users = [user['alt'] for user in dom_comments_users]

    publish([[url, dom_title, dom_op, dom_views]], 'video', videoId)
    publish([list(x) for x in zip(*[dom_comments_messages, dom_comments_users])], 'comment', videoId)
    publish(dom_tags, 'tag', videoId)
    publish(dom_categories, 'category', videoId)

    return 'Scraped {}'.format(str(url))

if __name__ == '__main__':
    from flask import Flask, request
    app = Flask(__name__)

    # option 1
    @app.route('/', methods=['POST', 'GET'])
    def test():
        return scrape_vid(request)

    # option 2
    app.add_url_rule('/scrape_vid', 'scrape_vid', scrape_vid, methods=['POST', 'GET'], defaults={'request': request})

    app.run(host='127.0.0.1', port=8088, debug=True)