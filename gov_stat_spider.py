import gc
import json
import re
import time
from hashlib import md5
from typing import Dict, Union, List, Any
from datetime import datetime

import requests

import pymysql
from lxml import etree

from user_agent_info import user_agent_list


class GovDataSpider(object):
    host_url = 'data.stats.gov.cn'
    base_url = 'https://{}/easyquery.htm?'
    base_xpath = ''
    db_conn = None
    db_cursor = None
    header_dict = {
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh-TW;q=0.8,zh-HK;q=0.6,en-US;q=0.4,ja;q=0.2",
        "Dnt": "1",
        "Connection": "keep-alive",
        "Host": host_url,
        'User-Agent': user_agent_list[0], }
    cookie_jar = None
    proxy_dict = {
        'http': 'http://localhost:34729',
        'https': 'http://localhost:34729',
        'socks': 'http://localhost:34728', }

    def get_base_html(self) -> Union[str, None]:
        try:
            url = self.base_url.format(
                self.host_url, 'cn=A01')
            self.header_dict['Accept'] = (
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8')
            resp = requests.get(
                url=url, headers=self.header_dict, timeout=30, verify=False, proxies=self.proxy_dict)
            html_text = resp.content.decode("UTF8", "ignore")
            self.cookie_jar = resp.cookies
            return html_text
        except Exception as e:
            print(e)
            return None

    def _get_zb_menu(
            self, menu_dict: Union[Dict[str, Any], None] = None) -> Union[
        List[Dict[str, Any]], None]:
        try:
            url = self.base_url.format(self.host_url)
            self.header_dict['Accept'] = 'application/json'
            self.header_dict['Content-Type'] = 'application/x-www-form-urlencoded'
            default_data = {
                "id": "zb",
                "dbcode": "hgyd",
                "wdcode": "zb",
                "m": "getTree", }
            if menu_dict:
                default_data['id'] = menu_dict.get('id')
            resp = requests.post(
                url=url, data=default_data,
                cookies=self.cookie_jar, headers=self.header_dict,
                timeout=30, verify=False, proxies=self.proxy_dict)
            return json.loads(resp.content)
        except Exception as e:
            print(e)
            return []

    def _get_zb_data(
            self, menu_dict: Union[Dict[str, Any], None] = None) -> Union[
        List[Dict[str, Any]], None]:
        try:
            url = self.base_url.format(self.host_url)
            self.header_dict['Accept'] = 'application/json'
            code = None
            if 'id' in menu_dict:
                code = menu_dict.get('id')
            elif 'code' in menu_dict:
                code = menu_dict.get('code')
            default_data = {
                "m": 'QueryData',
                "dbcode": 'hgyd',
                "rowcode": 'zb',
                "colcode": 'sj',
                "wds": '[]',
                "dfwds": '[{"wdcode":"zb","valuecode":"'+code+'"}]',
                "k1": int(round(time.time()*1000)),
                "h": '1', }
            resp = requests.get(
                url=url, params=default_data,
                cookies=self.cookie_jar, headers=self.header_dict,
                timeout=30, verify=False, proxies=self.proxy_dict)
            return json.loads(resp.content)
        except Exception as e:
            print(e)
            return []

    def get_zb(
            self, menu_dict: Union[Dict[str, Any], None] = None,
            **params) -> Union[List[Dict[str, Any]], None]:
        if menu_dict and not menu_dict.get('isParent'):
            if params and params.get('ignore_data'):
                return []
            else:
                return self._get_zb_data(menu_dict)
        else:
            if params and params.get('ignore_menu'):
                return []
            else:
                return self._get_zb_menu(menu_dict)

    def _menu_exists(self, menu_dict: Dict[str, Any]) -> bool:
        sql = "select id from hgyd_menu where id='{}'".format(
            menu_dict.get('id'))
        res_count = self.db_cursor.execute(sql)
        return True if res_count else False

    def _node_exists(self, node_dict: Dict[str, Any]) -> bool:
        sql = "select code from hgyd_node where code='{}'".format(
            node_dict.get('code'))
        res_count = self.db_cursor.execute(sql)
        return True if res_count else False

    def _data_exists(self, data_dict: Dict[str, Any]) -> bool:
        sql = "select code from hgyd_data where code='{}'".format(
            data_dict.get('code'))
        res_count = self.db_cursor.execute(sql)
        return True if res_count else False

    def menu_exists(self, menu_dict: Dict[str, Any]) -> bool:
        self.get_db_conn()
        res = self._menu_exists(menu_dict=menu_dict)
        self.close_db_conn()
        return res

    def node_exists(self, node_dict: Dict[str, Any]) -> bool:
        self.get_db_conn()
        res = self._node_exists(node_dict=node_dict)
        self.close_db_conn()
        return res

    def data_exists(self, data_dict: Dict[str, Any]) -> bool:
        self.get_db_conn()
        res = self._data_exists(data_dict=data_dict)
        self.close_db_conn()
        return res

    def insert_menu(self, menu_list: List[Dict[str, Any]]):
        self.get_db_conn()
        for menu_dict in menu_list:
            if not self._menu_exists(menu_dict):
                try:
                    ins_menu = ("insert into hgyd_menu"
                                " (id, db_code, is_parent, name, pid, wd_code, active)"
                                " values ('{}', '{}', {}, '{}', '{}', '{}', {});").format(
                        menu_dict.get('id'),
                        menu_dict.get('dbcode'),
                        1 if menu_dict.get('isParent') else 0,
                        menu_dict.get('name'),
                        menu_dict.get('pid') if len(menu_dict.get('pid')) > 0 else None,
                        menu_dict.get('wdcode'),
                        1, )
                    ins_menu = ins_menu.replace("'None'", "NULL")
                    ins_menu = ins_menu.replace("''", "NULL")
                    self.db_cursor.execute(ins_menu)
                    self.db_conn.commit()
                except Exception as ex:
                    print(ex)
                    self.db_conn.rollback()
                    continue
        self.close_db_conn()

    def insert_node(self, node_list: List[Dict[str, Any]]):
        self.get_db_conn()
        for node_dict in node_list:
            if not self._node_exists(node_dict):
                try:
                    ins_node = ("INSERT INTO gov_data_stat.hgyd_node"
                                " (code, name, dot_count, `exp`, if_show_code, memo, node_sort,"
                                " sort_code, tag, unit, active)"
                                " VALUES('{}', '{}', {}, '{}', {}, '{}',"
                                " '{}', {}, '{}', '{}', {});").format(
                        node_dict.get('code'),
                        node_dict.get('name'),
                        node_dict.get('dotcount'),
                        node_dict.get('exp') if len(node_dict.get('exp')) > 0 else None,
                        1 if node_dict.get('ifshowcode') else 0,
                        node_dict.get('memo'),
                        node_dict.get('nodesort'),
                        node_dict.get('sortcode'),
                        node_dict.get('tag') if len(node_dict.get('tag')) > 0 else None,
                        node_dict.get('unit'),
                        1, )
                    ins_node = ins_node.replace("'None'", "NULL")
                    ins_node = ins_node.replace("''", "NULL")
                    self.db_cursor.execute(ins_node)
                    self.db_conn.commit()
                except Exception as ex:
                    print(ex)
                    self.db_conn.rollback()
                    continue
        self.close_db_conn()

    def insert_data(self, data_list: List[Dict[str, Any]]):
        self.get_db_conn()
        for data_dict in data_list:
            if not self._data_exists(data_dict):
                try:
                    zb_code = sj_code = ""
                    for wd_dict in data_dict.get('wds'):
                        if wd_dict.get('wdcode') == 'zb':
                            zb_code = wd_dict.get('valuecode')
                        elif wd_dict.get('wdcode') == 'sj':
                            sj_code = wd_dict.get('valuecode')
                    ins_data = ("INSERT INTO gov_data_stat.hgyd_data"
                                " (code, `data`, str_data, dot_count, has_data, zb_code, sj_code)"
                                " VALUES('{}', {}, '{}', {}, {}, '{}', '{}');").format(
                        data_dict.get('code'),
                        data_dict.get('data').get('data'),
                        data_dict.get('data').get('strdata') if len(
                            data_dict.get('data').get('strdata')) > 0 else None,
                        data_dict.get('data').get('dotcount'),
                        1 if data_dict.get('data').get('hasdata') else 0,
                        zb_code,
                        sj_code, )
                    ins_data = ins_data.replace("'None'", "NULL")
                    ins_data = ins_data.replace("''", "NULL")
                    self.db_cursor.execute(ins_data)
                    self.db_conn.commit()
                except Exception as ex:
                    print(ex)
                    self.db_conn.rollback()
                    continue
        self.close_db_conn()

    def get_menu_from_db(
            self, condition_dict: Dict[str, Any]) -> Union[List[Dict[str, Any]], None]:
        self.get_db_conn()
        sql = ("SELECT id, db_code, is_parent, name, pid, wd_code, active "
               "FROM gov_data_stat.hgyd_menu")
        sql = _merge_sql_condition(sql=sql, condition_dict=condition_dict)
        self.db_cursor.execute(sql)
        res = self.db_cursor.fetchall()
        self.close_db_conn()
        return [{
            'id': x[0],
            'dbcode': x[1],
            'isParent': True if x[2] else False,
            'name': x[3],
            'pid': x[4],
            'wdcode': x[5],
            'active': True if x[6] else False,
        } for x in res] if len(res) > 0 else []

    def get_node_from_db(
            self, condition_dict: Dict[str, Any]) -> Union[List[Dict[str, Any]], None]:
        self.get_db_conn()
        sql = ("SELECT code, name, dot_count, `exp`, if_show_code, "
               "memo, node_sort, sort_code, tag, unit, active "
               "FROM gov_data_stat.hgyd_node")
        sql = _merge_sql_condition(sql=sql, condition_dict=condition_dict)
        self.db_cursor.execute(sql)
        res = self.db_cursor.fetchall()
        self.close_db_conn()
        return [{
            'code': x[0],
            'cname': x[1],
            'name': x[1],
            'dotcount': x[2],
            'exp': x[3],
            'ifshowcode': True if x[4] else False,
            'memo': x[5],
            'nodesort': x[6],
            'sortcode': x[7],
            'tag': x[8],
            'unit': x[9],
            'active': True if x[10] else False,
        } for x in res] if len(res) > 0 else []

    def get_db_conn(self):
        if not self.db_conn:
            self.db_conn = pymysql.connect(
                host='localhost', user='johann', password='Johann',
                database='gov_data_stat', charset='utf8', )
            self.db_cursor = self.db_conn.cursor()

    def close_db_conn(self):
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
            self.db_cursor = None
        gc.collect()

    def collect_menu_records(self):
        new_menu_list = self.get_zb()
        all_menu_list = []
        while len(new_menu_list) > 1:
            all_menu_list.extend(new_menu_list)
            loop_menu_list = new_menu_list.copy()
            new_menu_list = []
            for new_menu in loop_menu_list:
                if new_menu.get('isParent'):
                    new_menu_list.extend(self.get_zb(new_menu, ignore_data=True))
                time.sleep(5)
        self.insert_menu(all_menu_list)

    def _get_zb_by_code(self, input_code: Union[str, None]=None):
        if input_code and len(input_code) > 0:
            check_menu_list = self.get_node_from_db({
                'code': ['like', '%{}%'.format(input_code)],
                'active': ['=', 1], })
        else:
            check_menu_list = self.get_node_from_db({
                'active': ['=', 1], })
        raw_res_list = []
        for menu_dict in check_menu_list:
            res = self.get_zb(menu_dict=menu_dict)
            if not res:
                print('不能获取远程数据！')
                continue
            elif res.get('returncode') != 200:
                print(res.get('returndata'))
                continue
            else:
                raw_res_list.append(res)
            time.sleep(5)
        return raw_res_list

    def collect_node_records(self, input_code: Union[str, None]=None):
        raw_res_list = self._get_zb_by_code(input_code=input_code)
        all_node_list = []
        for res in raw_res_list:
            wd_node_list = res.get('returndata').get('wdnodes')
            for wd_node in wd_node_list:
                # 只收集指标节点
                if wd_node.get('wdcode') == 'zb':
                    all_node_list.extend(wd_node.get('nodes'))
        self.insert_node(all_node_list)

    def collect_data_records(self, input_code: Union[str, None]=None):
        raw_res_list = self._get_zb_by_code(input_code=input_code)
        all_data_list = []
        for res in raw_res_list:
            data_list = res.get('returndata').get('datanodes')
            all_data_list.extend(data_list)
        self.insert_data(all_data_list)

    def collect_multi_node_records(self):
        menu_dict_list = self.get_menu_from_db({
            'char_length(id)': ['=', 5],
            'active': ['=', 1], })
        menu_id_list = [menu_dict.get('id') for menu_dict in menu_dict_list]
        for menu_id in menu_id_list:
            self.collect_node_records(input_code=menu_id)
            print('{} 完成！'.format(menu_id))

    def collect_multi_data_records(self):
        print('输入抽取的menu_id：')
        input_code = input().strip()
        if input_code:
            node_dict_list = self.get_node_from_db({
                'code': ['like', '%{}%'.format(input_code)],
                'active': ['=', 1], })
        else:
            node_dict_list = self.get_node_from_db({
                'active': ['=', 1], })
        node_code_list = [node_dict.get('code') for node_dict in node_dict_list]
        for code in node_code_list:
            self.collect_data_records(input_code=code)
            print('{} 完成！'.format(code))

    def collect_multi_data_by_list(self):
        input_code_list = [
            'A010F', 'A010G', 'A0201', 'A0202', 'A0203', 'A0204', 'A0205', 'A0206',]
        for input_code in input_code_list:
            node_dict_list = self.get_node_from_db({
                'code': ['like', '%{}%'.format(input_code)],
                'active': ['=', 1], })

            node_code_list = [node_dict.get('code') for node_dict in node_dict_list]
            for code in node_code_list:
                self.collect_data_records(input_code=code)
                print('{} 完成！'.format(code))

    def run(self):
        html = self.get_base_html()
        print('输入操作指令：')
        input_code = input().strip()
        if len(input_code) == 1:
            input_code = int(input_code)
            func_dict = {
                1: self.collect_menu_records,
                2: self.collect_multi_node_records,
                3: self.collect_multi_data_records,
                4: self.collect_multi_data_by_list, }
            func_dict[input_code]()
        else:
            menu_dict = {'id': input_code}
            res = self._get_zb_data(menu_dict=menu_dict)
            print(res)


def _merge_sql_condition(sql: str, condition_dict: Dict[str, Any]) -> str:
    key_list = [k for k in condition_dict.keys()]
    if condition_dict and len(condition_dict) > 0:
        for i in range(len(condition_dict)):
            sql += " Where" if i == 0 else " And"
            sql += " {} {} {}".format(
                key_list[i],
                condition_dict.get(key_list[i])[0],
                condition_dict.get(key_list[i])[1] if isinstance(
                    condition_dict.get(key_list[i])[1], (int, float)) else "'{}'".format(
                    condition_dict.get(key_list[i])[1]), )
    return sql


if __name__ == '__main__':
    start = datetime.now()
    # 实例化一个对象spider
    spider = GovDataSpider()
    spider.run()
    end = datetime.now()
    # 查看程序执行时间
    print('执行时间:{}'.format(end - start))
