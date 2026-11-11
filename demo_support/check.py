import json,xml.etree.ElementTree as ET
from pathlib import Path
from decimal import Decimal
ROOT=Path(__file__).resolve().parents[1]
a=ET.parse(ROOT/'fixtures/receive.xml').getroot();b=ET.parse(ROOT/'fixtures/transform.xml').getroot()
assert a.tag=='{urn:manufacturing:1}Exchange';assert b.tag=='{urn:manufacturing:2}Exchange'
mapping=json.loads((ROOT/'fixtures/codes.json').read_text());assert Decimal(mapping['units']['g']['factor'])*10000==10
assert b[0].attrib['sequence']=='1' and a[0].attrib['sequence']=='0'
print('制造 XML 两版本、前序及单位样本检查通过')
