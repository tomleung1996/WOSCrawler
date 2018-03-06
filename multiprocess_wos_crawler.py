import requests
import re
import os
import pymysql
import time
import random
import multiprocessing
from retrying import retry
from random import choice
from bs4 import BeautifulSoup



def pause_some_time(a,b):
    """
    定义进行随机等待的函数

    :param a: --- int，随机值（randint）的下限
    :param b: --- int，随机值（randint）的上限
    :return: None
    """
    sec = random.random() * random.randint(a, b)
    print('\t\t\t睡眠', sec, '秒')
    time.sleep(sec)
    return


def get_sid(session):
    """
    定义获取SID的函数，通过访问http://www.webofknowledge.com/，
    可以在URL或者COOKIES中提取所需的SID

    :param session: --- requests.Session，用来维持一个SID为本函数所获得的值的会话
    :return sid: --- str，所获得的SID
    """
    session.get('http://www.webofknowledge.com',timeout=25)
    sid = session.cookies['SID'].replace('"', '')
    print('\t获得SID：', sid)
    return sid


# 进行搜索
def get_search_result(session, sid, anum):
    """
    定义进行搜索的函数，接收搜索的关键字，并返回检索结果。
    默认检索的字段是Web of Science核心合集的入藏号（UT），该字段可以唯一标识一篇文章
    如需检索其他字段，请将search_formdata中value(select1)的值换成想要检索字段的简称，
    如标题是TI

    如无必要，请勿改动header，search_headers仅包含了必要的字段，并已经通过了爬取测试

    :param session: --- requests.Session，用来维持一个SID为本函数所获得的值的会话
    :param sid: --- str,与session对应的SID
    :param anum: --- str，检索关键字，目前为Web of Science核心合集入藏号
    :return search_result: --- request.Response，检索结果页面的响应内容
    """
    search_url = 'http://apps.webofknowledge.com/WOS_GeneralSearch.do'

    def get_search_header(sid, anum):
        """
        定义构造检索时所需Headers和表单的函数，表单具体字段意义基本同其名字

        :param sid:  --- str,父函数的SID
        :param anum:  --- str，父函数的检索关键字，目前为Web of Science核心合集入藏号
        :return search_headers, search_formdata:  --- tuple(dic,dic)，[0]为所需的Header，[1}为需要提交的表单
        """
        search_headers = {
            'User-Agent': User_Agent,
            'Referer': 'http://apps.webofknowledge.com/WOS_GeneralSearch_input.do?product=WOS&SID=' + sid + '&search_mode=GeneralSearch',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        search_formdata = {
            'fieldCount': 1,
            'action': 'search',
            'product': 'WOS',
            'search_mode': 'GeneralSearch',
            'SID': sid,
            'max_field_count': 25,
            'max_field_notice': '注意: 无法添加另一字段。',
            'input_invalid_notice': '检索错误: 请输入检索词。',
            'exp_notice': '检索错误: 专利检索词可在多个家族中找到',
            'input_invalid_notice_limits': ' <br/>注: 滚动框中显示的字段必须至少与一个其他检索字段相组配。',
            'sa_params': 'WOS||' + sid + '|http://apps.webofknowledge.com|',
            'formUpdated': 'TRUE',
            'value(input1)': anum,
            'value(select1)': 'UT',
            'value(hidInput1)': '',
            'limitStatus': 'collapsed',
            'ss_lemmatization': 'On',
            'ss_spellchecking': 'Suggest',
            'SinceLastVisit_UTC': '',
            'SinceLastVisit_DATE': '',
            'period': 'Range Selection',
            'range': 'ALL',
            'startYear': '1986',
            'endYear': '2018',
            'update_back2search_link_param': 'yes',
            'ssStatus': 'display:none',
            'ss_showsuggestions': 'ON',
            'ss_query_language': 'auto',
            'ss_numDefaultGeneralSearchFields': 1,
            'rs_sort_by': 'PY.D;LD.D;SO.A;VL.D;PG.A;AU.A',
        }
        return search_headers, search_formdata

    headers, formdata = get_search_header(sid, anum)
    print('\t\t正在获取关键字为', anum, '的检索结果页面')
    time.sleep(random.random())
    search_result = session.post(search_url, data=formdata, headers=headers,timeout=25)
    print('\t\t检索结果页面状态码及链接：', search_result.status_code, search_result.url)
    return search_result


# 访问施引文献页面
def get_citation_result(session, search_result):
    """
    定义根据搜索结果获得施引文献结果的函数，
    施引文献结果即普通检索结果中点击被引频次数字所跳转的页面
    ！！！请注意，本函数运行时，保证每一篇文献都至少有1篇施引文献才能正常工作，使用前请先在数据库中
    ！！！处理好，按被引数（TC）降序排列或暂时去除被引数为0的文献

    :param session:  --- requests.Session，用来维持一个连续的会话
    :param search_result: --- request.Response，检索结果页面的响应内容
    :return citation_result, cite_num: --- tuple(requests.Response,int)，[0]为施引文献页面的响应结果，[1]为实际的施引文献数（小于等于数据库中TC的值）
    """

    def get_citation_link(search_result):
        """
        根据检索结果的响应找到施引文献结果的链接的函数
        ！！！如果文献被引数为空的话此函数可能会出错，需要改写下方bs4的find语句

        :param search_result: --- request.Response，检索结果页面的响应内容
        :return citation_link: --- str，施引文献页面的链接
        """
        soup = BeautifulSoup(search_result.text, 'lxml')
        citedata = soup.find(class_='search-results-data-cite')
        citation_a = citedata.select('a')[0]
        citation_tmp_link = citation_a['href']
        citation_link = 'http://apps.webofknowledge.com' + citation_tmp_link
        print('\t\t施引文献页面链接：', citation_link)
        return citation_link

    def get_citation_header(search_result):
        """
        构造访问施引文献结果所需的Headers，不需要提交表单

        :param search_result: --- request.Response，检索结果页面的响应内容
        :return citation_headers: --- dic，所需的Header
        """
        citation_headers = {

            'User-Agent': User_Agent,
            'Referer': search_result.url
        }
        return citation_headers

    citation_headers = get_citation_header(search_result)
    citation_link = get_citation_link(search_result)
    citation_result = session.get(citation_link, headers=citation_headers,timeout=25)
    soup = BeautifulSoup(citation_result.text, 'lxml')
    cite_num = int(soup.select('#footer_formatted_count')[0].string.replace(',', ''))
    print('\t\t\t施引文献总数:', cite_num)
    return citation_result, cite_num


# 正式下载
def get_output_result(session, sid, citation_result, start_num, end_num, filename):
    """
    实际执行下载的函数

    :param session: --- requests.Session，用来维持一个连续的会话
    :param sid: --- str,与session对应的SID
    :param citation_result: --- requests.Response，施引文献页面的响应结果
    :param start_num: --- int，导出结果的起始记录序号
    :param end_num: --- int，导出结果的终止记录序号（最多一次导出500条）
    :param filename: --- str，下载保存的文件绝对路径
    :return: None
    """

    def get_qids(citation_result):
        """
        提取导出结果所必须的qid信息
        Web of Science通过SESSION、SID、Qid以及ParentQid来进行检索结果的返回，
        所以之前进行的每一步操作都要环环相扣

        :param citation_result: --- requests.Response，施引文献页面的响应结果
        :return parentQid, qid: --- tuple(str,str)，[0]为parentQid，[1]为qid
        """
        soup = BeautifulSoup(citation_result.text, 'lxml')
        qids = soup.select('#currUrl')[0]['value']
        parentQid = re.search('parentQid=\\w+&', qids)[0].replace('parentQid=', '').replace('&', '')
        qid = re.search('qid=\\w+&', qids)[0].replace('qid=', '').replace('&', '')
        return parentQid, qid


    def get_output_header(citation_link, qid, sid, startNum, endNum):
        """
        构造导出引文所需要的Header的函数

        :param citation_link: --- str，施引文献页面的链接，用来放入Referer字段，很重要
        :param qid: --- str，qid
        :param sid: --- str,与session对应的SID
        :param startNum: --- int，导出结果的起始记录序号
        :param endNum: --- int，导出结果的终止记录序号（最多一次导出500条）
        :return output_headers, output_formdata: --- tuple(dic,dic)，[0]为所需的Header，[1}为需要提交的表单
        """

        output_headers = {

            'User-Agent': User_Agent,
            'Referer': citation_link,
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        output_formdata = {
            'selectedIds': '',
            'displayCitedRefs': 'true',
            'displayTimesCited': 'true',
            'displayUsageInfo': 'true',
            'viewType': 'summary',
            'product': 'WOS',
            'rurl': 'http%3A%2F%2Fapps.webofknowledge.com%2Fsummary.do%3Fproduct%3DWOS%26search_mode%3DCitingArticles%26parentQid%3D1%26page%3D2%26qid%3D2%26SID%3D' + sid + '%26parentProduct%3DWOS',
            'mark_id': 'WOS',
            'colName': 'WOS',
            'search_mode': 'CitingArticles',
            'locale': 'en_US',
            'view_name': 'WOS-CitingArticles-summary',
            'sortBy': 'PY.D;LD.D;SO.A;VL.D;PG.A;AU.A',
            'mode': 'OpenOutputService',
            'qid': qid,
            'SID': sid,
            'format': 'saveToFile',
            'filters': 'HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND AUTHORSIDENTIFIERS ACCESSION_NUM FUNDING SUBJECT_CATEGORY JCR_CATEGORY LANG IDS PAGEC SABBR CITREFC ISSN PUBINFO KEYWORDS CITTIMES ADDRS CONFERENCE_SPONSORS DOCTYPE CITREF ABSTRACT CONFERENCE_INFO SOURCE TITLE AUTHORS  ',
            'mark_to': endNum,
            'mark_from': startNum,
            'queryNatural': None,
            'count_new_items_marked': '0',
            'use_two_ets': 'false',
            'IncitesEntitled': 'yes',
            'value(record_select_type)': 'range',
            'markFrom': startNum,
            'markTo': endNum,
            'fields_selection': 'HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND AUTHORSIDENTIFIERS ACCESSION_NUM FUNDING SUBJECT_CATEGORY JCR_CATEGORY LANG IDS PAGEC SABBR CITREFC ISSN PUBINFO KEYWORDS CITTIMES ADDRS CONFERENCE_SPONSORS DOCTYPE CITREF ABSTRACT CONFERENCE_INFO SOURCE TITLE AUTHORS  ',
            'save_options': 'html'
        }
        return output_headers, output_formdata

    citation_link = citation_result.url
    qid = get_qids(citation_result)[1]
    startNum = start_num
    endNum = end_num
    output_headers, output_formdata = get_output_header(citation_link, qid, sid, startNum, endNum)
    print('\t\t\t尝试获取第', startNum, '条到第', endNum, '条结果')
    download_result = session.post('http://apps.webofknowledge.com/OutboundService.do?action=go&&',
                                   data=output_formdata, headers=output_headers, timeout=35)
    if (download_result.status_code == 200):
        print('\t\t\t第', startNum, '条到第', endNum, '条结果下载成功，下载URL：', download_result.url)
    else:
        print('\t\t\t！！！第', startNum, '条到第', endNum, '条结果下载失败！！！', download_result.status_code, download_result.url)
        raise Exception('下载失败，重试！')
    with open(filename + '.html', 'a', encoding='utf-8') as file:
        file.write(download_result.text)
    return



def get_docs_info(start, step):
    """
    从数据库中取出被引文献存入队列以供使用的函数，根据自己数据库的结构进行修改

    :param start: --- int，开始的记录编号（0为第一行）
    :param step: --- int，步进幅度，一次性取多少行
    :return: None
    """
    global docs_queue
    db = pymysql.connect('localhost', 'user', 'password', 'db_name')
    cursor = db.cursor()
    cursor.execute(
        'SELECT non_zero_ID,articleID,UT,TC FROM article_tc_non_zero ORDER BY non_zero_ID ASC LIMIT ' + str(start) + ', ' + str(step))
    docs_result = cursor.fetchall()
    for doc in docs_result:
        docs_queue.put(doc)
    db.close()
    return



@retry(wait_fixed=5000,stop_max_attempt_number=2)
def get_one_doc_all_output_result(doc_tuple):
    """
    整合函数，完成获取一条被引文献记录的全部施引文献工作
    每一环节都允许失败重试一次，若再次失败则调用retry等待5秒后再重试
    ！！！这部分代码不够健壮，需要改进，若网站由于某些原因访问不了可能会浪费很多时间

    :param doc_tuple: --- tuple(str,str,str,str)，数据库中的字段，根据自己的数据库格式修改，最重要的是UT字段，其他字段主要用作文件命名格式
    :return: None
    """
    global SESSION
    global SID
    global User_Agent
    global t_init

    if(time.time()-t_init>1700):
        SESSION = requests.session()
        SID = get_sid(SESSION)
        User_Agent = choice(UA)
        print('此SID和SESSION已使用接近30分钟，更换为新SID：',SID)
        t_init=time.time()

    print('使用SID：',SID)

    non_zero_ID = str(doc_tuple[0])
    article_ID = str(doc_tuple[1])
    anum = str(doc_tuple[2])

    try:
        search_result = get_search_result(SESSION, SID, anum)
    except:
        print('出错，等待后重试')
        pause_some_time(1,5)
        search_result = get_search_result(SESSION, SID, anum)

    pause_some_time(1,3)

    try:
        citation_result, cite_num = get_citation_result(SESSION, search_result)
    except:
        print('出错，等待后重试')
        pause_some_time(1,3)
        citation_result, cite_num = get_citation_result(SESSION, search_result)

    pause_some_time(1,3)
    tc = str(cite_num)
    path = os.path.dirname(
        os.getcwd()) + os.path.sep + 'download' + os.path.sep + non_zero_ID + '-' + article_ID + '-' + tc + '-' + anum.replace(
        ':', '')
    i = 1
    while (i <= cite_num):
        pause_some_time(1,3)
        try:
            get_output_result(SESSION, SID, citation_result, i, i + 500 - 1, path)
        except:
            print('出错，等待后重试')
            pause_some_time(1,3)
            get_output_result(SESSION, SID, citation_result, i, i + 500 - 1, path)
        i = i + 500
    return



def mainprocess(doc):
    """
    包装一下上面的整合函数，加入时间统计

    :param doc: --- tuple(str,str,str,str)，数据库中的字段，根据自己的数据库格式修改，最重要的是UT字段，其他字段主要用作文件命名格式
    :return: None
    """
    t0 = time.time()
    print('正在抓取《', str(doc[0]) + '-' + str(doc[1]) + '-' + str(doc[3]) + '-' + str(doc[2]).replace(':', ''), '》的施引文献')
    get_one_doc_all_output_result(doc)
    print('本条用时', time.time() - t0, '秒')
    pause_some_time(1,5)
    return


UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.32',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 BIDUBrowser/8.3 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.9.2.1000 Chrome/39.0.2146.0 Safari/537.36',
    'Mozilla/5.0 (compatible; Googlebot/2.1; +https://www.google.com/bot.html)',
)

SESSION = requests.session()
SID = get_sid(SESSION)
User_Agent = choice(UA)
t_init=time.time()
docs_queue = multiprocessing.Queue()
get_docs_info(1,4)

if __name__=='__main__':
    t1 = time.time()
    #Pool里面的为进程数，32进程测试通过，但是在施引文献数很大时不建议使用太多进程（4-8左右）
    #在施引文献数普遍在50条以下时，可以采用更多进程，具体进程数需要自己调试
    pool=multiprocessing.Pool(4)
    while(docs_queue.empty()!=True):
        pool.apply_async(mainprocess,(docs_queue.get(),))
    pool.close()
    pool.join()
    print('执行完毕')
    print('全部用时', time.time() - t1, '秒')
