import tarfile
import re
import fnmatch
import smtplib
import subprocess
import sys
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from loguru import logger
import datetime
from omegaconf import DictConfig
import pymupdf
import pymupdf.layout
pymupdf.TOOLS.mupdf_display_errors(False)
pymupdf.layout.activate()

import pymupdf4llm  # noqa: E402

PDF_EXTRACT_SCRIPT = r"""
from pathlib import Path
import sys

import pymupdf
import pymupdf.layout
import pymupdf4llm

pymupdf.TOOLS.mupdf_display_errors(False)
pymupdf.layout.activate()

file_path = sys.argv[1]
output_path = Path(sys.argv[2])
markdown = pymupdf4llm.to_markdown(
    file_path,
    use_ocr=False,
    header=False,
    footer=False,
    ignore_code=True,
)
output_path.write_text(markdown, encoding="utf-8")
"""

def extract_tex_code_from_tar(file_path:str, paper_id:str) -> dict[str,str | None] | None:
    try:
        tar = tarfile.open(file_path)
    except tarfile.ReadError:
        logger.debug(f"Failed to find main tex file of {paper_id}: Not a tar file.")
        return None
 
    tex_files = [f for f in tar.getnames() if f.endswith('.tex')]
    if len(tex_files) == 0:
        logger.debug(f"Failed to find main tex file of {paper_id}: No tex file.")
        tar.close()
        return None
    
    bbl_file = [f for f in tar.getnames() if f.endswith('.bbl')]
    match len(bbl_file) :
        case 0:
            if len(tex_files) > 1:
                logger.debug(f"Cannot find main tex file of {paper_id} from bbl: There are multiple tex files while no bbl file.")
                main_tex = None
            else:
                main_tex = tex_files[0]
        case 1:
            main_name = bbl_file[0].replace('.bbl','')
            main_tex = f"{main_name}.tex"
            if main_tex not in tex_files:
                logger.debug(f"Cannot find main tex file of {paper_id} from bbl: The bbl file does not match any tex file.")
                main_tex = None
        case _:
            logger.debug(f"Cannot find main tex file of {paper_id} from bbl: There are multiple bbl files.")
            main_tex = None

    if main_tex is None:
        logger.debug(f"Trying to choose tex file containing the document block as main tex file of {paper_id}")
    #read all tex files
    file_contents = {}
    for t in tex_files:
        extracted = tar.extractfile(t)
        if extracted is None:
            logger.debug(f"Failed to read tex file {t} of {paper_id}: File not found in tar.")
            continue
        with extracted as f:
            content = f.read().decode('utf-8',errors='ignore')
        #remove comments
        content = re.sub(r'%.*\n', '\n', content)
        content = re.sub(r'\\begin{comment}.*?\\end{comment}', '', content, flags=re.DOTALL)
        content = re.sub(r'\\iffalse.*?\\fi', '', content, flags=re.DOTALL)
        #remove redundant \n
        content = re.sub(r'\n+', '\n', content)
        content = re.sub(r'\\\\', '', content)
        #remove consecutive spaces
        content = re.sub(r'[ \t\r\f]{3,}', ' ', content)
        if main_tex is None and re.search(r'\\begin\{document\}', content) and not any(w in t for w in ['example', 'sample']):
            main_tex = t
            logger.debug(f"Choose {t} as main tex file of {paper_id}")
        file_contents[t] = content
    
    if main_tex is not None:
        main_source:str = file_contents[main_tex]
        #find and replace all included sub-files
        include_files = re.findall(r'\\input\{(.+?)\}', main_source) + re.findall(r'\\include\{(.+?)\}', main_source)
        for f in include_files:
            if not f.endswith('.tex'):
                file_name = f + '.tex'
            else:
                file_name = f
            main_source = main_source.replace(f'\\input{{{f}}}', file_contents.get(file_name, ''))
        file_contents["all"] = main_source
    else:
        logger.debug(f"Failed to find main tex file of {paper_id}: No tex file containing the document block.")
        file_contents["all"] = None
        
    tar.close()
    return file_contents

def extract_markdown_from_pdf(file_path:str) -> str:
    return pymupdf4llm.to_markdown(file_path,use_ocr=False,header=False,footer=False,ignore_code=True)


def extract_markdown_from_pdf_with_timeout(file_path: str, timeout: int, output_path: str) -> str | None:
    try:
        subprocess.run(
            [sys.executable, "-c", PDF_EXTRACT_SCRIPT, file_path, output_path],
            check=True,
            timeout=timeout,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"PDF extraction timed out for {file_path}")
        return None
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.strip() if e.stderr else str(e)
        logger.warning(f"Failed to extract markdown from pdf {file_path}: {stderr}")
        return None

    with open(output_path, encoding="utf-8") as f:
        return f.read()

def glob_match(path:str, pattern:str) -> bool:
    path_parts = [] if path == '' else path.split('/')
    pattern_parts = [] if pattern == '' else pattern.split('/')

    def _match(pattern_index: int, path_index: int) -> bool:
        if pattern_index == len(pattern_parts):
            return path_index == len(path_parts)

        current_pattern = pattern_parts[pattern_index]
        if current_pattern == '**':
            return _match(pattern_index + 1, path_index) or (
                path_index < len(path_parts) and _match(pattern_index, path_index + 1)
            )

        return (
            path_index < len(path_parts)
            and fnmatch.fnmatchcase(path_parts[path_index], current_pattern)
            and _match(pattern_index + 1, path_index + 1)
        )

    return _match(0, 0)

def send_email(config:DictConfig, html:str):
    sender = config.email.sender
    receiver = config.email.receiver
    password = config.email.sender_password
    smtp_server = config.email.smtp_server
    smtp_port = config.email.smtp_port
    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    msg = MIMEText(html, 'html', 'utf-8')
    msg['From'] = _format_addr('Github Action <%s>' % sender)
    msg['To'] = _format_addr('You <%s>' % receiver)
    today = datetime.datetime.now().strftime('%Y/%m/%d')
    msg['Subject'] = Header(f'Daily arXiv {today}', 'utf-8').encode()

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
    except Exception as e:
        logger.debug(f"Failed to use TLS. {e}\nTry to use SSL.")
        try:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        except Exception as e:
            logger.debug(f"Failed to use SSL. {e}\nTry to use plain text.")
            server = smtplib.SMTP(smtp_server, smtp_port)

    server.login(sender, password)
    server.sendmail(sender, [receiver], msg.as_string())
    server.quit()
