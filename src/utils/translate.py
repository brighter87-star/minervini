import time

import requests
from src.utils.config import NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, PAPAGO_TRANSLATE_URL


class TranslationError(Exception):
    def __init__(self, status_code, error_code=None, message=None, response_text=None):
        self.status_code = status_code
        self.error_code = error_code
        self.message = message or "Translation failed"
        self.response_text = response_text
        super().__init__(f"[{status_code}][{error_code}] {self.message}")


RETRY_STATUS = {500, 502, 503, 504}
MAX_RETRY = 4


def translate_txt_papago(text: str, source="en", target="ko") -> str:
    headers = {
        "x-ncp-apigw-api-key-id": NAVER_CLIENT_ID,
        "x-ncp-apigw-api-key": NAVER_CLIENT_SECRET,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    if not text:
        return ""

    for attempt in range(1, MAX_RETRY):
        backoff = 1
        try:
            res = requests.post(
                PAPAGO_TRANSLATE_URL,
                headers=headers,
                params={
                    "source": source,
                    "target": target,
                    "text": text,
                },
                timeout=20,
            )
        except requests.RequestException as e:
            if attempt == MAX_RETRY:
                raise TranslationError(-1, "NETWORK", str(e)) from e
            time.sleep(backoff)
            backoff *= 2
            continue
        if res.status_code == 200:
            return res.json()["message"]["result"]["translatedText"]

        error_code = None
        error_msg = None
        try:
            ej = res.json().get("error", {})
            error_code = ej.get("errorCode")
            error_msg = ej.get("message")
        except Exception:
            pass

        # 5xx면 재시도
        if res.status_code in RETRY_STATUS:
            if attempt == MAX_RETRY:
                raise TranslationError(res.status_code, error_code, error_msg, res.text)
            time.sleep(backoff)
            backoff *= 2
            continue

        # 4xx 등은 즉시 예외
        raise TranslationError(res.status_code, error_code, error_msg, res.text)


def main():
    text = "Test"
    translate_txt_papago(text=text, source="en", target="ko")


if __name__ == "__main__":
    main()
