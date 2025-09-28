import requests, json, re, time

from bs4 import BeautifulSoup
from pathlib import Path


from src.utils.config import SEC_CIK_URL

HEADERS = {
    "User-Agent": "MinerviniResearchBot/0.1 AdminContact: brighter87@gmail.com",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


def get_cik_code(ticker: str) -> str:

    cik_mapper_path = Path("src/data/cached/cik/cik_mapper.json")

    with cik_mapper_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    for v in data.values():
        if v["ticker"].upper() == ticker.upper():
            return str(v["cik_str"]).zfill(10)

    raise ValueError("CIK not found")

def get_latest_filing_urls(cik: str, forms=("10-K"), take=3):
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    data = requests.get(url, headers=HEADERS).json()
    recent_filings = data["filings"]["recent"]
    out = []

    for f, acc, doc, date in zip(recent_filings["form"],
                                 recent_filings["accessionNumber"],
                                 recent_filings["primaryDocument"],
                                 recent_filings["filingDate"]):
        if f in forms:
            acc_no_dash = acc.replace("-", "")
            url_html = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no_dash}/{doc}"
            out.append({
                "form": f,
                "date": date,
                "accession": acc,
                "url": url_html
                })
            if len(out) >= take: break
    return out

def parse_business_section_from_doc(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=25)
    if res.status_code != 200:
        raise RuntimeError(f"{res.status_code} | {res.text[:300]}")
    soup = BeautifulSoup(res.text, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=False)
    text = text.replace("\u00A0", " ").replace("\u2002", " ")
    text = re.sub(r"[ \t]{1,}", " ", text)
    text = re.sub(r"\n{2,}", r"\n", text)

    business_pattern: Pattern[str] = re.compile(
    r"(\bItem\s*[0-9A-Za-z]+[\. \s]*Business\n\b)(.*\n)(.*\n)",
    flags=re.I | re.M
    )

    extracted = business_pattern.findall(text)

    if not extracted:
        print("맞는 패턴이 없어요.")
        return None

    print(extracted)
    return extracted

    # m_end = end_pat.search(text, m_start.end())

    # return text[m_start.end():m_start.end()+100].strip()
    """
    if m_end:
        return text[m_start.end():m_end.start()].strip()
    else:
        return text[m_start.end():].strip()
    """


def main():
    ticker = "QUBT"
    cik_code = get_cik_code(ticker)
    out = get_latest_filing_urls(cik=cik_code)
    text = parse_business_section_from_doc(out[0]["url"])
    print(text)

if __name__ == "__main__":
    main()
