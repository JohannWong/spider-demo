"""
电影天堂  最新电影  网页内容  详见 https://www.dytt8.net/html/gndy/dyzz/index.html
<div class="co_content8">
<ul>
<td height="220" valign="top">
<table width="100%" border="0" cellspacing="0" cellpadding="0" class="tbspan" style="margin-top:6px">
    <tr>
        <td height="1" colspan="2" background="/templets/img/dot_hor.gif"></td>
    </tr>
    <tr>
        <td width="5%" height="26" align="center"><img src="/templets/img/item.gif" width="18" height="17"></td>
        <td height="26">
            <b>
                <a href="/html/gndy/dyzz/20231003/64189.html" class="ulink">
                2022年剧情《世间有她》BD国语中字
                </a>
            </b>
        </td>
    </tr>
    <tr>
        <td height="20" style="padding-left:3px">&nbsp;</td>
        <td style="padding-left:3px">
            <font color="#8F8C89">日期：2023-10-03 09:49:44
            </font>
        </td>
    </tr>
    <tr>
        <td colspan="2" style="padding-left:3px">
        ◎译 名 世间有她 / Hero / HerStory ◎年 代 2022 ◎产 地 中国大陆 ◎类 别 剧情 ◎语 言 普通话 ◎字 幕 中文 ◎上映日期 2022-09-09(中国大陆) ◎豆瓣评分 5.5/10 from 28791 users ◎片 长 116分钟 ◎导 演 李少红 Shaohong Li 陈冲 Joan Chen 张艾嘉 Sylvia Chan
        </td>
    </tr>
</table>
    ...
    ...
    ...
</td>
</ul>
</div>
"""
import gc
import re
from hashlib import md5
from typing import Dict, Union, List
from datetime import datetime

import requests

import pymysql
from lxml import etree

from user_agent_info import user_agent_list


class DyttSpider(object):
    host_url = '23.225.199.194'
    base_url = 'https://{}/html/gndy/dyzz/list_23_{}.html'
    base_xpath = '//div[@class="co_content8"]/ul/td/table'
    begin_page_no = 1
    end_page_no = 10
    db_conn = None
    db_cursor = None
    header_dict = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh-TW;q=0.8,zh-HK;q=0.6,en-US;q=0.4,ja;q=0.2",
        "Dnt": "1",
        "Connection": "keep-alive",
        "Host": host_url,
        "Cookie": "track_info=1696920522971",
        'User-Agent': user_agent_list[0], }

    def get_html(self, page_no: int) -> Union[str, None]:
        try:
            url = self.base_url.format(self.host_url, page_no)
            resp = requests.get(
                url=url, headers=self.header_dict, timeout=10, verify=False)
            html_text = resp.content.decode("GBK", "ignore")
            return html_text
        except Exception as e:
            print(e)
            return None

    def data_parse(
            self, html: str) -> Union[List[Dict[str, Union[str, int]]], None]:
        try:
            parse_html = etree.HTML(html)
            data_list = parse_html.xpath(self.base_xpath)
            movie_data_list = []
            for data in data_list:
                origin_date = data.xpath('./tr[3]/td[2]/font/text()')[0].strip()
                re_date = r'\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}'
                pattern = re.compile(re_date, re.S)
                date_time = datetime.strptime(
                    pattern.findall(origin_date)[0], '%Y-%m-%d %H:%M:%S')
                movie_data = {
                    "movie_name": data.xpath('./tr[2]/td[2]/b/a/text()')[0].strip(),
                    "movie_link": "https://{}{}".format(
                        self.host_url,
                        data.xpath('./tr[2]/td[2]/b/a/@href')[0].strip()),
                    "movie_date": date_time,
                    "movie_detail": data.xpath('./tr[4]/td[1]/text()')[0].strip(),
                }
                movie_data_list.append(movie_data)
            return movie_data_list
        except Exception as ex:
            print(ex)
            return None

    def finger_exists(self, movie_finger: str) -> bool:
        sql = "select finger from movie_finger where finger='{}'".format(movie_finger)
        res_count = self.db_cursor.execute(sql)
        return True if res_count else False

    def insert_movie_data(
            self, movie_data: Dict[str, Union[str, int]],
            movie_finger: Union[str, None] = None):
        if not self.db_conn:
            self.db_conn = pymysql.connect(
                host='localhost', user='johann', password='Johann',
                database='dytt_movie', charset='utf8', )
            self.db_cursor = self.db_conn.cursor()
        if not movie_finger:
            md5_s = md5()
            md5_s.update(movie_data.get('movie_link').encode())
            movie_finger = md5_s.hexdigest()
        try:
            ins_finger = "insert into movie_finger (finger) values ('{}')".format(movie_finger)
            self.db_cursor.execute(ins_finger)
            ins_movie = ("insert into movie_data "
                         "(movie_name, movie_link, datetime, detail) "
                         "values ('{}', '{}', '{}', '{}')").format(
                movie_data.get('movie_name'), movie_data.get('movie_link'),
                movie_data.get('movie_date'), movie_data.get('movie_detail'))
            self.db_cursor.execute(ins_movie)
        except Exception as ex:
            print(ex)
            self.db_conn.rollback()
        self.db_conn.commit()

    def close_db_conn(self):
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
            self.db_cursor = None
        gc.collect()

    def handel_movie_data(self, movie_data_list: List[Dict[str, Union[str, int]]]):
        for movie_data in movie_data_list:
            md5_s = md5()
            md5_s.update(movie_data.get('movie_link').encode())
            movie_finger = md5_s.hexdigest()
            if not self.finger_exists(movie_finger):
                self.insert_movie_data(movie_data, movie_finger)
        self.close_db_conn()

    def run(self):
        movie_data_list = []
        for page_no in range(
                self.begin_page_no, self.end_page_no+1):
            html = self.get_html(page_no)
            if not html:
                continue
            new_data_list = self.data_parse(html)
            if not new_data_list:
                continue
            movie_data_list.extend(new_data_list)
            print('第{}页抓取成功'.format(page_no))
        # print(movie_data_list)
        self.handel_movie_data(movie_data_list)


if __name__ == '__main__':
    start = datetime.now()
    # 实例化一个对象spider
    spider = DyttSpider()
    spider.run()
    end = datetime.now()
    # 查看程序执行时间
    print('执行时间:{}'.format(end-start))
