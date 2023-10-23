from urllib import request

user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    ' Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
    ' Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.47',
]


def run_main():
    for user_agent in user_agent_list:
        headers = {
            "Accept": "application/json",
            "User-Agent": user_agent}
        url = 'http://httpbin.org/get'
        req = request.Request(url=url, headers=headers, method="GET")
        res = request.urlopen(req)
        html = res.read().decode('utf-8')
        print(html)


if __name__ == '__main__':
    run_main()
