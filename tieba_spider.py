from typing import Dict, Union
from urllib import request, parse
import time
import random

from user_agent_info import user_agent_list


class TiebaSpider(object):
    base_url = 'http://c.tieba.baidu.com/f/good?{}'
    name = '弱智'
    begin_page_no = 1
    end_page_no = 5

    def get_html(self, params: Dict[str, Union[str, int]]) -> str:
        params = parse.urlencode(params)
        url = self.base_url.format(params)
        req = request.Request(
            url=url, headers={
                'Accept': 'text/html',
                'User-Agent': random.choice(user_agent_list)},
            method="GET")
        res = request.urlopen(req)
        html = res.read().decode("utf-8", "ignore")
        return html

    def run(self):
        for page_no in range(
                self.begin_page_no, self.end_page_no+1):
            pn = (page_no-1)*50
            params = {
                "kw": self.name,
                "pn": pn,
                "cid": 3,
                "ie": "utf-8"
            }
            html = self.get_html(params)
            print('第{}页抓取成功'.format(page_no))
            print(html)


if __name__ == '__main__':
    start = time.time()
    # 实例化一个对象spider
    spider = TiebaSpider()
    spider.run()
    end = time.time()
    # 查看程序执行时间
    print('执行时间:{}'.format(end-start))
