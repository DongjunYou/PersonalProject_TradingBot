import pandas as pd
import pymysql
from bs4 import BeautifulSoup
import urllib, pymysql, calendar, time, json  # urllib은 기본 내장
from urllib.request import urlopen
from threading import Timer #실행스레드 관리(지정시간동안 대기)
from datetime import datetime


class DBupdater:
    def __init__(self):  # 클래스 인식용 # 특정 작업을 하기 전 사전 작업
        self.conn = pymysql.connect(host='localhost',user='root',password='98lutris',db='stockinvestor',charset='utf8')  #connector객체 #한글회사명위해 인코딩
        with self.conn.cursor() as cursor:  # with로 임시객체 #self는 클래스내 메서드가 클래스 지칭할때
            sql='''
            CREATE TABLE IF NOT EXISTS company_info(  # 미리 만들필요 없게
                code VARCHAR(20),
                company VARCHAR(40),
                last_update Date,
                PRIMARY KEY (code))            
            '''
            cursor.execute(sql)
            sql = '''
            CREATE TABLE IF NOT EXISTS daily_price(  
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code,date))            
                '''
            cursor.execute(sql)
        self.conn.commit()  # autocommit 디폴트가 false임
        self.codes=dict() # 빈 딕셔너리 생성
        self.update_info()


    def __del__(self):  # 연결해제
        self.conn.close()


    def read_code(self): # 1.krx에서 종목코드 읽어오기
        url='https://dev-kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13' #다운로드찾기타입 13!!
        krx=pd.read_html(url,header=0,flavor='lxml')[0]  # header로 첫 행이 컬럼명, flavor로 파서지정 # [0]이 기본
        krx=krx[['종목코드','회사명']]  #파일보니깐 선정
        krx=krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code=krx.code.map('{:06d}'.format) # 형식통제
        return krx


    def update_info(self):  # 2.종목코드 업데이트
        sql = 'SELECT * FROM company_info'
        df = pd.read_sql(sql, self.conn)  # 테이블을 판다스로 읽어오는 방법
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
        with self.conn.cursor() as cursor:
            sql = 'SELECT max(last_update) FROM company_info'
            cursor.execute(sql)
            db = cursor.fetchone()  # db의 최신날짜(첫줄) #fetchone함수인데 괄호없다고 suscriptable찡찡대서 한참 찾음
            today = datetime.today().strftime('%Y-%m-%d')
            if db[0] == None or db[0].strftime('%Y-%m-%d') < today:  # db가 outdated라면
                krx = self.read_code()  # 앞선 read_code 클래스
                for idx in range(len(krx)): #dataframe행개수
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info(code, company, last_update) VALUES ('{code}','{company}','{today}')" #포맷팅
                    cursor.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}]{idx:04d} REPLACE INTO company_info VALUES({code},{company},{today})")  # 업데이트 할때 띄우기 위해
                self.conn.commit()


    def read_naver(self,code,company,pages_to_fetch): # 3.네이버에서 스크레이핑
        url=f'http://finance.naver.com/item/sise_day.nhn?code={code}'
        with urlopen(url) as doc: #urlopen으로 html문서
            if doc is None: return None
            html=BeautifulSoup(doc,'lxml')
            pgrr=html.find('td', class_='pgRR')  # Beautifulsoup의 의의는 find함수로 html일부분객체(직접 문서보고 태그 찾기) #중복될수도 있으니까_
            if pgrr is None: return None
            s=str(pgrr.a["href"]).split('=') #url의 맨 뒷부분
            lastpage=s[-1]
        df=pd.DataFrame()
        pages=min(int(lastpage),pages_to_fetch) #직접 설정해준 쪽이 작으면 그걸로 가라
        for page in range(1,pages+1): #갯수만 같게
            pg_url='{}&page{}'.format(url,page) #fstring과 똑같은 방식
            df=df.append(pd.read_html(pg_url,header=0)[0])
            tmnow=datetime.now().strftime('%Y-%m-%d %H:%M')
            print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow,company,code,page,pages),end='\r')
            df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})
            df['date']=df['date'].replace('.','-')
            df=df.dropna()
            df[['close','diff','open','high','low','volume']]=df[['close','diff','open','high','low','volume']].astype(int)
            df=df[['date','open','high','low','close','diff','volume']]
        return df


    def replace_db(self,df,num,code,company):  # 4.스크레이핑해온 데이터로 최신화
        with self.conn.cursor() as cursor:
            for r in df.itertuples():
                sql=f"REPLACE INTO daily_price VALUES ('{code}','{r.date}','{r.open}','{r.high}','{r.low}','{r.close}','{r.diff}','{r.volume}')"
                cursor.execute(sql)
            self.conn.commit()
            print('[()] #{:0.4d} {} ({}) : {} rows > REPLACE INTO daily_price [OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'),num+1,company,code,len(df)))


    def update_price(self,pages_to_fetch):  # 5.전체 주식시세 읽어 업데이트
        for idx, code in enumerate(self.codes):  #update_info로 코드담은 딕셔너리
            df=self.read_naver(code,self.codes[code],pages_to_fetch)
            if df is None : continue
            self.replace_db(df,idx,code,self.codes[code])

    def execute_daily(self):
        self.update_info()
        try:
            with open('config.json','r') as in_file: #json모듈로 설정 인코딩 디코딩(기본파일에 저장)
                config=json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError: #에러도 중요한 일부
            with open('config.json','w') as out_file:
                pages_to_fetch=100
                config={'page_to_fetch':1}
                json.dump(config, out_file)
        self.update_price(pages_to_fetch)

        tmnow=datetime.now()
        lastday=calendar.monthrange(tmnow.year,tmnow.month)[1] #말일 구하는 함수
        if tmnow.month==12 and tmnow.day==lastday: #해가 바뀌면
            tmnext=tmnow.replace(year=tmnow.year+1,month=1,day=1,hour=17,minute=0,second=0)
        elif tmnow.day==lastday:
            tmnext=tmnow.replace(month=tmnow.month+1,day=1,hour=17,minute=0,second=0)
        else:
            tmnext=tmnow.replace(day=tmnow.day+1,hour=17,minute=0,second=0)
        tmdiff=tmnext-tmnow
        secs=tmdiff.seconds

        t=Timer(secs,self.execute_daily())
        print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()


if __name__=='__main__':
    dbu=DBupdater()
    dbu.execute_daily()
