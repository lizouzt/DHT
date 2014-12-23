# -*- coding: utf-8 -*-
import pdb
import json
import logging
import urllib2, re, os, datetime
from string import Template
from urllib2 import HTTPError

logging.basicConfig(level=logging.DEBUG,
    format='%(asctime)s [line: %(lineno)d] %(levelname)s %(message)s',
    datefnt='%d %b %H:%M:%S',
    filename='spider.log',
    filemode='a')

#定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class Spider:
    outputJsonFile = './data/'
    # ----页面配置----
    site = "http://www.yetwo.com/"
    cur_category = ''
    home_category = "dianying/"
    # 分页规则 index.html index2.html index3.html
    page = "index${num}.html"
    page_template = Template(site + home_category + page)
    total_page = 1060
    
    # 多余内容切割点
    category_page_start_cut_point = ""
    category_page_end_cut_point = "电影索引"
    
    # ----图片配置----
    image_start_cut_point = "o_big_img_bg_b"
    image_end_cut_point = "o_r_wap"
    image_pattern = r'<img src=".*?" title'
    # ----标题配置----
    title_start_cut_point = "名称："
    title_end_cut_point = "演员："
    title_pattern = r'<h1>.*?</h1>'
    # ----演员配置----
    actor_start_cut_point = "演员："
    actor_end_cut_point = "导演："
    actor_pattern = r'<a href=".*?" target="_blank">.*?</a> '
    # ----导演配置----
    director_start_cut_point = "导演："
    director_end_cut_point = "类型："
    director_pattern = r'<a href=".*?" target="_blank">.*?</a>'
    # ----地区配置----
    area_start_cut_point = "类型"
    area_end_cut_point = "上映"
    area_pattern = r'<li>.*?</li>'
    # ----年份配置----
    date_start_cut_point = "上映"
    date_end_cut_point = "状态"
    date_pattern = '[0-9]{4}'
    # ----内容配置----
    content_start_cut_point = "omov_list3"
    content_end_cut_point = "</div>"
    content_pattern = r'>.*?</div>'
    
    def catch_page(self, url):
        try:
            request = urllib2.Request(url)
            response = urllib2.urlopen(request)
            page_content = response.read()
            return page_content
        except Exception, e:
            logging.error('catch_page error: ' + str(Exception) + str(e) + url);
            return False
    
    def cut_by_str(self, page_content, cut_point, left_or_right='right', reverse=False):
        if cut_point != '':
            if reverse:
                str_index = page_content.rfind(cut_point)
            else:
                str_index = page_content.find(cut_point)
                
            if left_or_right == 'left':
                page_content = page_content[:str_index]
            elif left_or_right == 'right':
                page_content = page_content[str_index + 1:]
            
        return page_content
    
    def catch_by_pattern(self, pattern, page_content, one_or_more='one'):
        if one_or_more == 'one':
            m = re.search(pattern, page_content)
            if m is not None:
                return m.group()
            else:
                return None
        elif one_or_more == 'more':
            return re.findall(pattern, page_content)
        
    def filter_by_patterns(self, result, *patterns):
        for pattern in patterns:
            matchs = self.catch_by_pattern(pattern, result, 'more')
            for item in matchs:
                result = result.replace(item, '')
        return result
    
    def catch_something(self, pattern, page_content, one_or_more='one'):
        result = self.catch_by_pattern(pattern, page_content, one_or_more)
        
    def catch_image(self, pattern, page_content):
        pic_url = self.catch_by_pattern(pattern, page_content, 'one')
        if pic_url is not None:
            pic_url = self.cut_by_str(pic_url, "\"")
            pic_url = self.cut_by_str(pic_url, "\"", 'left', True)

            response = self.catch_page(pic_url)

            pic_name = self.cut_by_str(pic_url, "/", reverse=True)
            
            if pic_name is None or len(pic_name) < 4:
                return 'null'
            
            path = "image/" + self.cur_category + '/'

            if not os.path.exists(path):
                os.makedirs(path)
            # pdb.set_trace()
            path = path + pic_name
            with open(path, 'wb') as pic:
                pic.write(response)

            return pic_name
            
    def catch_title(self, pattern, page_content):
        title = self.catch_by_pattern(pattern, page_content, 'one')
        if title is not None:
            title = self.filter_by_patterns(title, r'<\S[^>]+>')
            return title
        
    def catch_actor(self, pattern, page_content):
        actor_list = self.catch_by_pattern(pattern, page_content, 'more')
        result_list = []
        for actor in actor_list:
            actor = self.filter_by_patterns(actor, r'<\S[^>]+>')
            result_list.append(actor)
        return result_list
        
    def catch_director(self, pattern, page_content):
        director_list = self.catch_by_pattern(pattern, page_content, 'more')
        result_list = []
        for director in director_list:
            director = self.filter_by_patterns(director, r'<\S[^>]+>')
            result_list.append(director)
        return result_list
        
    def catch_area(self, pattern, page_content):
        area_content = self.catch_by_pattern(pattern, page_content)
        if area_content is not None:
            area_content = self.filter_by_patterns(area_content, r'<\S[^>]+>')
            area_content = area_content.replace('地区：', '')
            return area_content
        
    def catch_date(self, pattern, page_content):
        date_content = self.catch_by_pattern(pattern, page_content)
        if date_content is not None:
            return date_content
        
    def catch_intro(self, pattern, page_content):
        intro = self.catch_by_pattern(pattern, page_content)
        if intro is not None:
            intro = self.filter_by_patterns(intro, r'<\S[^>]+>', r'&[a-z]*?;')
            return intro
        
    def catch_url(self, pattern, page_content):
        #走bt接口获取磁链接地址
        pass
            
    def catch(self):
        # 获取每一页的电影链接
        for page_index in range(self.total_page):
            if page_index == 0:
                current_url = self.page_template.safe_substitute(num='')
            else:
                current_url = self.page_template.safe_substitute(num=page_index + 1)
            
            page_content = self.catch_page(current_url)
            
            if page_content == False:
                continue

            # 根据切割点截取多余内容
            page_content = self.cut_by_str(page_content, self.category_page_start_cut_point)
            page_content = self.cut_by_str(page_content, self.category_page_end_cut_point, 'left')
            # 获取每部电影的链接 ：href="dongzuo/31632/"
            match_list = self.catch_by_pattern(r'[a-zA-Z]+\/[0-9]+\/', page_content, 'more')
            # 去除重复元素
            match_list = list(set(match_list))

            # 获取每部电影的详细信息页面
            for item in match_list:
                detail_url = self.site + item
                page_content = self.catch_page(detail_url)
                
                if page_content == False:
                    continue

                film_dict = {}

                ret = re.search(r'([a-zA-Z]+)\/(\d+)\/', item)
                # pdb.set_trace()
                film_info = ret.groups()
                if not len(film_info) == 2:
                    logging.warning('failed with get Type&ID: ' + item)
                    continue

                self.cur_category = film_info[0]
                film_dict['type'] = film_info[0]
                film_dict['id'] = film_info[1]

                # 获取影片标题
                title_content = self.cut_by_str(page_content, self.title_start_cut_point)
                title_content = self.cut_by_str(title_content, self.title_end_cut_point, 'left')
                title = self.catch_title(self.title_pattern, title_content)
                print 'title:', title
                film_dict['title'] = title

                # 获取图片
                image_content = self.cut_by_str(page_content, self.image_start_cut_point)
                image_content = self.cut_by_str(image_content, self.image_end_cut_point, 'left')
                image = self.catch_image(self.image_pattern, image_content)
                print 'image:', image
                film_dict['image'] = image
                
                # 获取演员
                actor_content = self.cut_by_str(page_content, self.actor_start_cut_point)
                actor_content = self.cut_by_str(actor_content, self.actor_end_cut_point, 'left')
                actor_list = self.catch_actor(self.actor_pattern, actor_content)
                print 'actors:', len(actor_list)
                film_dict['actors'] = actor_list

                # 获取导演
                director_content = self.cut_by_str(page_content, self.director_start_cut_point)
                director_content = self.cut_by_str(director_content, self.director_end_cut_point, 'left')
                director_list = self.catch_director(self.director_pattern, director_content)
                film_dict['directors'] = director_list
                
                # 获取地区
                area_content = self.cut_by_str(page_content, self.area_start_cut_point)
                area_content = self.cut_by_str(area_content, self.area_end_cut_point, 'left')
                area = self.catch_area(self.area_pattern, area_content)
                film_dict['area'] = area
                
                # 获取上映时间
                year_content = self.cut_by_str(page_content, self.date_start_cut_point)
                year_content = self.cut_by_str(year_content, self.date_end_cut_point, 'left')
                year = self.catch_date(self.date_pattern, year_content)
                film_dict['year'] = year
                
                # 获取内容简介
                content_content = self.cut_by_str(page_content, self.content_start_cut_point)
                content_content = self.cut_by_str(content_content, self.content_end_cut_point, 'left')
                content = self.catch_intro(self.content_pattern, content_content)
                film_dict['abstract'] = content
                
                path = "data/" + self.cur_category + '/'
                if not os.path.exists(path):
                    os.makedirs(path)

                dbname = path + film_dict['id'] + '.json'
                with open(dbname, 'w') as fout:
                    fout.write(json.dumps(film_dict, ensure_ascii=False))
                    print 'oooutputted!'
    
if __name__ == "__main__":
    Spider().catch()
