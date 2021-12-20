import fitz
import pdfplumber
from timeit import default_timer as timer
from datetime import timedelta
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from tika import parser
import tika
tika.initVM()

def pdf_fitz(full_path):
    pdf = fitz.open(full_path)
    result = []
    all_page_count = pdf.page_count
    for page in range(all_page_count):
        result.append(str(pdf.get_page_text(page)).upper() + '\n')
        if page % 1000 == 0 and page != 0:
            txt = ','.join(result)
            print(txt)
            result = []

    txt = ','.join(result)
    print(txt)

def pdf_plumber(full_path):
    pdf = pdfplumber.open(full_path)
    result = []
    for num in range(len(pdf.pages)):
        page = pdf.pages[num]
        result.append(str(page.extract_text()).upper()+'\n')
        if num % 1000 == 0 and num != 0:
            txt = ','.join(result)
            print(txt)
            result = []
    txt = ','.join(result)
    print(txt)

def pdf_miner(full_path):
    output_string = StringIO()
    with open(full_path, 'rb') as f:
        parser = PDFParser(f)
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
    print(str(output_string.getvalue()))

def pdf_tika(full_path):
    headers = {
                "X-Tika-OCRLanguage": "eng",
                "X-Tika-OCRTimeout": "300"
                                            }
    parsed = parser.from_file(full_path, xmlContent=False, requestOptions={'headers':headers, 'timeout':300})
    act = parsed['content']
    if act != None:
        txt = act.strip().upper()
    else:
        txt = ''
    parsed.clear()
    print(txt)

if __name__ == '__main__':
    start = timer()

    full_path = r'C:\Users\hjkim\OneDrive\바탕 화면\새 폴더\산학협력 활동 우수사례_신구대학교.pdf'
    f = open(full_path, 'rb')
    buf = f.read()

    print(pdf_fitz(buf))

    end = timer()
    print(timedelta(seconds=end - start))
