import re, sys
from collections import defaultdict
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

STATIONS = {'1 NHK総合1..':'NHK G Osaka','4 MBS':'MBS','6 ABCテレビ1':'ABC','7 テレビ大阪1':'TV Osaka','8 関西テレビ1':'Kansai TV','10 読売テレビ1':'ytv','3 サンテレビ1':'SUN'}
JST = ZoneInfo('Asia/Tokyo')

class Guide(HTMLParser):
    def __init__(self):
        super().__init__(); self.in_headers=False; self.line=None; self.capture=None; self.headers=[]; self.rows=defaultdict(list); self.row=None
    def handle_starttag(self, tag, attrs):
        a=dict(attrs)
        if tag=='div' and a.get('id')=='ch_area': self.in_headers=True
        if tag=='ul' and re.fullmatch(r'program_line_\d+',a.get('id','')): self.line=int(a['id'].split('_')[-1])
        if tag=='li' and self.line and re.fullmatch(r'\d{12}', a.get('s','')) and re.fullmatch(r'\d{12}',a.get('e','')): self.row=[a['s'],a['e'],'']
        if tag=='p' and (self.in_headers or (self.row and 'program_title' in a.get('class','').split())): self.capture=[]
    def handle_data(self,text):
        if self.capture is not None: self.capture.append(text)
    def handle_endtag(self,tag):
        if tag=='p' and self.capture is not None:
            t=''.join(self.capture).strip(); self.capture=None
            if self.in_headers and re.match(r'^\d+\s',t): self.headers.append(t)
            elif self.row: self.row[2]=t
        if tag=='div' and self.in_headers: self.in_headers=False
        if tag=='li' and self.row:
            self.rows[self.line].append(self.row); self.row=None
        if tag=='ul' and self.line: self.line=None

def parse(html):
    p=Guide();p.feed(html)
    if any(p.headers.count(n)!=1 for n in STATIONS): raise ValueError('Osaka lineup missing or ambiguous')
    for i,n in enumerate(p.headers,1):
        if n in STATIONS:
            for s,e,t in p.rows[i]:
                a,b=(datetime.strptime(x,'%Y%m%d%H%M').replace(tzinfo=JST) for x in (s,e))
                if t and a<b<=a+timedelta(days=1): yield STATIONS[n],a,b,t

def build(htmls):
    root=ET.Element('tv'); rows=defaultdict(set)
    for html in htmls:
        for n,a,b,t in parse(html): rows[n].add((a,b,t))
    for n in STATIONS.values():
        if not rows[n]: continue
        ch=ET.SubElement(root,'channel',id=n); ET.SubElement(ch,'display-name').text=n
        for a,b,t in sorted(rows[n]):
            x=ET.SubElement(root,'programme',channel=n,start=a.strftime('%Y%m%d%H%M%S %z'),stop=b.strftime('%Y%m%d%H%M%S %z'));ET.SubElement(x,'title',lang='ja').text=t
    return root

def fetch_days():
    now=datetime.now(JST)-timedelta(hours=5)
    for i in range(4):
        date=(now+timedelta(days=i)).strftime('%Y%m%d')
        url=f'https://bangumi.org/epg/td?broad_cast_date={date}&ggm_group_id=84'
        with urlopen(Request(url,headers={'User-Agent':'Mozilla/5.0'}),timeout=15) as r: yield r.read().decode('utf-8')

if __name__=='__main__':
    root=build((Path(x).read_text() for x in sys.argv[1:]) if len(sys.argv)>1 else fetch_days())
    ET.ElementTree(root).write(sys.stdout.buffer,encoding='utf-8',xml_declaration=True)
