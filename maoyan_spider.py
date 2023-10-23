"""
猫眼电影网  top100榜  网页内容  详见 https://www.maoyan.com/board/4
<dl class="board-wrapper">
  <dd>
    <i class="board-index board-index-1">1</i>
    <a href="/films/1200486" title="我不是药神" class="image-link" data-act="boarditem-click" data-val="{movieId:1200486}">
      <img src="//s3.meituan.net/static-prod01/com.sankuai.movie.fe.mywww-files/image/loading_2.e3d934bf.png" alt="" class="poster-default">
      <img alt="我不是药神" class="board-img" src="https://p0.pipi.cn/mmdb/d2dad59253751bd236338fa5bd5a27c710413.jpg?imageView2/1/w/160/h/220">
    </a>
    <div class="board-item-main">
      <div class="board-item-content">
        <div class="movie-item-info">
        <p class="name"><a href="/films/1200486" title="我不是药神" data-act="boarditem-click" data-val="{movieId:1200486}">我不是药神</a></p>
        <p class="star">
          主演：徐峥,周一围,王传君
        </p>
        <p class="releasetime">上映时间：2018-07-05</p>
        </div>
        <div class="movie-item-number score-num">
          <p class="score"><i class="integer">9.</i><i class="fraction">6</i></p>
        </div>
      </div>
    </div>
  </dd>
  ...
  ...
  ...
</dl>
"""
from typing import Dict, Union, List
from urllib import request, parse
import requests
import re
import time

from user_agent_info import user_agent_list


class MaoyanSpider(object):
    host_url = 'www.maoyan.com'
    base_url = 'https://{}/board/4?{}'
    re_str = ('<div class="movie-item-info">.*?title="(.*?)"'
              '.*?<p class="star">(.*?)</p>'
              '.*?<p class="releasetime">(.*?)</p>')
    begin_page_no = 1
    end_page_no = 3

    def get_html(self, params: Dict[str, Union[str, int]]) -> str:
        params = parse.urlencode(params)
        url = self.base_url.format(self.host_url, params)
        resp = requests.get(
            url=url, headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh-TW;q=0.8,zh-HK;q=0.6,en-US;q=0.4,ja;q=0.2",
                "Dnt": "1",
                "Connection": "keep-alive",
                "Host": self.host_url,
                'User-Agent': user_agent_list[0]}, )
        return resp.content.decode()

    def maoyan_top100_parse(self, html: str) -> List[Dict[str, Union[str, int]]]:
        pattern = re.compile(self.re_str, re.S)
        # r_list: [('我不是药神','徐峥,周一围,王传君','2018-07-05'),...] 列表元组
        r_list = pattern.findall(html)
        res_list = []
        for r_tmp in r_list:
            res_data = {
                'name': r_tmp[0],
                'star': r_tmp[1],
                'date': r_tmp[2]
            }
            res_list.append(res_data)
        return res_list

    def run(self):
        for page_no in range(
                self.begin_page_no, self.end_page_no+1):
            offset = (page_no-1)*10
            params = {
                "offset": offset
            }
            html = self.get_html(params)
            res_list = self.maoyan_top100_parse(html)
            print('第{}页抓取成功'.format(page_no))
            print(res_list)


if __name__ == '__main__':
    start = time.time()
    # 实例化一个对象spider
    spider = MaoyanSpider()
    spider.run()
    end = time.time()
    # 查看程序执行时间
    print('执行时间:{}'.format(end-start))
