import pandas as pd
import numpy as np

def screen_minervini(
    df: pd.DataFrame,
    rs_col: str = "RS_200_pct",     # 보유한 RS 컬럼명: RS_50_pct / RS_150_pct / RS_200_pct 중 선택
):
    latest = df.groupby("T").tail(1).copy()
 
    cond1 = (latest["c"] > latest["ma50"]) & (latest["c"] > latest["ma150"]) & (latest["c"] > latest["ma200"])
    cond2 = (latest["ma50"] > latest["ma150"]) & (latest["ma50"] > latest["ma200"])
    cond3 = (latest["ma150"] > latest["ma200"])
    cond4 = latest["is_ma200_up"]
    cond5 = latest["c"] >= (latest["52w_high"] * 0.75)  # 52주 고점의 75% 이상(= 고점 대비 -25% 이내)
    cond6 = latest["c"] >= (latest["52w_low"] * 1.30)   # 52주 저점 대비 +30% 이상
    cond7 = latest[rs_col] >= 0.70

    latest["meets_all"] = cond1 & cond2 & cond3 & cond4 & cond5 & cond6 & cond7
    selected = latest[latest["meets_all"]].copy()
    
    return selected.sort_values(by=rs_col, ascending=False)

def main():
    pass

if __name__ == "__main__":
    main()
