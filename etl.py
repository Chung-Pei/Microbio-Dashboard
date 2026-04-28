"""
etl.py — 學習數據分析儀表板前處理腳本
版本：v1.0
輸入：raw_data/ 資料夾下所有 .xls 檔案
輸出：docs/data.json

使用方式：
    python etl.py
    python etl.py --raw_dir 自訂資料夾 --out docs/data.json
"""

import os
import re
import json
import argparse
from datetime import datetime
from collections import defaultdict

import xlrd

# ── 路徑設定 ──────────────────────────────────────────────────
DEFAULT_RAW_DIR = "raw_data"
DEFAULT_OUT     = os.path.join("docs", "data.json")

# ── 異常標籤辭典 ──────────────────────────────────────────────
# 格式：(Regex pattern, color, label)
EXCEPTION_DICT = [
    # 🔴 重大違規（順序重要：長 pattern 在前，避免短 pattern 重複匹配）
    (r"被檢舉考試作弊",              "red",    "重大違規"),
    (r"(?<!被檢舉)考試作弊",         "red",    "重大違規"),
    # 🟡 學習態度異常
    (r"未達\s*24\s*小時",       "yellow", "學習態度異常"),
    (r"閱讀\s*[小少於]*\s*24\s*小時", "yellow", "學習態度異常"),
    (r"前後測未做",             "yellow", "學習態度異常"),
    (r"前測未做",               "yellow", "學習態度異常"),
    (r"後測沒做",               "yellow", "學習態度異常"),
    (r"筆試未達\s*21\s*分",     "yellow", "學習態度異常"),
    (r"筆試不及格",             "yellow", "學習態度異常"),
    (r"最高\s*56\s*分計",       "yellow", "學習態度異常"),
    (r"期中考補考打八折",        "yellow", "學習態度異常"),
    # 🔵 行政缺漏
    (r"同意書未簽",             "blue",   "行政缺漏"),
    (r"補同意書",               "blue",   "行政缺漏"),
    (r"無簽同意書",             "blue",   "行政缺漏"),
    # ⚪ 特殊身份（非異常）
    (r"小老師",                 "gray",   "特殊身份"),
]

# 成績欄位的異常文字（缺考/重考）→ 轉為數值並標記
SCORE_EXCEPTIONS = [
    (r"缺考", "yellow", "學習態度異常", "缺考"),
    (r"重考", "yellow", "學習態度異常", "重考"),
]

# 學號格式：9 位數字
RE_STUDENT_ID = re.compile(r"^\d{9}$")

# 末端班級代碼格式：如 "11121A 護二一Ａ" 或 "11231B 日二一乙"
RE_CLASS_CODE = re.compile(r"^(\d{5}[A-Za-z0-9])\s+(.+)$")

# 過濾用：時間戳（如「檢索:113.06.18 14:47」）
RE_TIMESTAMP = re.compile(r"檢索[:：]?\d{2,3}\.\d{2}\.\d{2}")

# 學期成績：需排除的統計列關鍵字
STAT_ROW_KEYWORDS = ["標準差", "平均", "全", "人;", "人，", "重修當人"]


# ══════════════════════════════════════════════════════════════
# 工具函式
# ══════════════════════════════════════════════════════════════

def mask_student_id(sid: str) -> str:
    """學號遮蔽：前3碼 + **** + 後2碼。學號不足5碼時全部遮蔽。"""
    if len(sid) >= 5:
        return sid[:3] + "****" + sid[-2:]
    return "*" * len(sid)  # 異常短學號：全遮蔽

def parse_score(val) -> tuple:
    """
    解析成績欄位值。
    回傳 (數值或None, 異常標籤列表)
    """
    exceptions = []
    if val == "" or val is None:
        return None, exceptions

    # 已是數字
    if isinstance(val, (int, float)):
        return float(val), exceptions

    text = str(val).strip()
    if text == "":
        return None, exceptions

    # 嘗試轉數字
    try:
        return float(text), exceptions
    except ValueError:
        pass

    # 比對異常文字
    for pattern, color, label, tag in SCORE_EXCEPTIONS:
        if re.search(pattern, text):
            exceptions.append({"tag": tag, "color": color, "label": label})
            return 0.0, exceptions

    # 無法識別 → 保留原始文字但回傳 None
    return None, exceptions

def parse_exceptions(text: str) -> list:
    """從備註文字解析異常標籤列表"""
    if not text or RE_TIMESTAMP.search(str(text)):
        return []
    tags = []
    for pattern, color, label in EXCEPTION_DICT:
        if re.search(pattern, str(text)):
            tags.append({"tag": label,   # 固定標籤文字，不帶入原始 XLS 內容以防前端 XSS
                         "color": color, "label": label})
    return tags

def parse_semester_from_filename(fname: str) -> str:
    """
    從檔名解析學期代碼。
    112_1__xxx.xls → "1121"
    113_2__xxx.xls → "1132"
    """
    m = re.match(r"(\d{3})_(\d)", os.path.basename(fname))
    if m:
        return m.group(1) + m.group(2)
    return "unknown"

def detect_sheet_type(sheet_name: str) -> str:
    """判斷分頁類型"""
    if "實驗" in sheet_name:
        return "practicum"
    if "暑期" in sheet_name:
        return "summer"
    return "theory"

def find_col(headers: list, *candidates) -> int:
    """在 header 列中找欄位索引，找不到回傳 -1"""
    for c in candidates:
        for i, h in enumerate(headers):
            if str(h).strip() == c:
                return i
    return -1

def is_valid_student_row(row_vals: list) -> bool:
    """判斷是否為有效學生資料列（學號欄為9位數字）"""
    if not row_vals:
        return False
    sid = str(row_vals[0]).strip()
    # xlrd 可能把數字學號讀成 float
    if re.match(r"^\d+\.\d+$", sid):
        sid = str(int(float(sid)))
    return bool(RE_STUDENT_ID.match(sid))

def normalize_student_id(raw) -> str:
    """統一學號格式為 9 位字串"""
    s = str(raw).strip()
    if re.match(r"^\d+\.\d+$", s):
        s = str(int(float(s)))
    return s.zfill(9)

def parse_class_from_cell(cell_text: str) -> tuple:
    """
    解析班級備註欄：
    "11121A 護二一Ａ" → ("11121A", "護二一Ａ")
    找不到格式則回傳 (None, None)
    """
    text = str(cell_text).strip()
    m = RE_CLASS_CODE.match(text)
    if m:
        return m.group(1), m.group(2)
    # 有些只有中文班級名（如「護二三丙」）
    if text and not any(c.isdigit() for c in text[:3]):
        return None, text
    return None, None


# ══════════════════════════════════════════════════════════════
# 正課 / 暑期 分頁解析
# ══════════════════════════════════════════════════════════════

def parse_theory_sheet(sheet, semester: str, sheet_type: str) -> list:
    """
    解析正課或暑期學分分頁。
    回傳 list of dict（每位學生一筆記錄）
    """
    records = []
    if sheet.nrows < 2:
        return records

    # 讀取 header 列（第 0 列）
    headers = [str(sheet.cell_value(0, c)).strip() for c in range(sheet.ncols)]

    # 定位欄位
    col_id       = find_col(headers, "學號")
    col_name     = find_col(headers, "姓名")
    col_mid      = find_col(headers, "期中考")
    col_final    = find_col(headers, "期末考")
    col_sem      = find_col(headers, "學期成績")
    col_adj      = find_col(headers, "調整")
    col_read     = find_col(headers, "閱讀%")
    col_attend   = find_col(headers, "簽到%")    # 暑期專用
    col_readhr   = find_col(headers, "閱讀時數")  # 暑期專用

    if col_id == -1 or col_sem == -1:
        return records  # 無法識別的分頁

    # 找備註欄：學號右側最後幾欄中，非空白且符合班級格式或辭典的欄
    # 策略：掃描所有欄，找「班級代碼欄」與「異常備註欄」
    def get_trailing_cells(row_vals):
        """取學期成績欄之後的所有非空白文字"""
        results = []
        start = max(col_sem, col_adj if col_adj != -1 else 0) + 1
        for c in range(start, len(row_vals)):
            v = str(row_vals[c]).strip()
            if v and not RE_TIMESTAMP.search(v):
                results.append(v)
        return results

    for r in range(1, sheet.nrows):
        row_vals = [sheet.cell_value(r, c) for c in range(sheet.ncols)]

        if not is_valid_student_row(row_vals):
            continue

        sid = normalize_student_id(row_vals[col_id])

        # 解析成績
        mid_score, mid_exc   = parse_score(row_vals[col_mid]   if col_mid   != -1 else "")
        fin_score, fin_exc   = parse_score(row_vals[col_final] if col_final != -1 else "")
        sem_score, sem_exc   = parse_score(row_vals[col_sem])
        adj_val,   _         = parse_score(row_vals[col_adj]   if col_adj   != -1 else "")
        read_val,  _         = parse_score(row_vals[col_read]  if col_read  != -1 else "")
        attend_val,_         = parse_score(row_vals[col_attend]if col_attend!= -1 else "")
        readhr_val,_         = parse_score(row_vals[col_readhr]if col_readhr!= -1 else "")

        # 彙整成績異常標籤
        exceptions = mid_exc + fin_exc + sem_exc

        # 解析尾端欄（班級代碼 + 備註）
        class_code_raw = None
        class_name     = None
        trailing = get_trailing_cells(row_vals)

        for cell_text in trailing:
            code, name = parse_class_from_cell(cell_text)
            if name and class_name is None:
                class_code_raw = code
                class_name     = name
            # 異常標籤
            exc = parse_exceptions(cell_text)
            exceptions.extend(exc)

        # 去重標籤
        seen = set()
        unique_exc = []
        for e in exceptions:
            key = e["tag"]
            if key not in seen:
                seen.add(key)
                unique_exc.append(e)

        record = {
            "student_id":      sid,
            "semester":        semester,
            "sheet_name":      sheet.name,
            "type":            sheet_type,
            "class_code_raw":  class_code_raw,
            "class_name":      class_name or sheet.name,
            "midterm":         mid_score,
            "final":           fin_score,
            "semester_score":  sem_score,
            "adjusted":        adj_val,
            "reading_pct":     read_val,
            # Online_Buffer 預留
            "attend_pct":      attend_val,
            "reading_hours":   readhr_val,
            "exceptions":      unique_exc,
            # 後續跨學期計算填入
            "is_retaker":      False,
            "delta":           None,
        }
        records.append(record)

    return records


# ══════════════════════════════════════════════════════════════
# 實驗課分頁解析
# ══════════════════════════════════════════════════════════════

def parse_practicum_sheet(sheet, semester: str) -> list:
    """
    解析實驗課分頁。
    擷取：期中考15、期末考30、總成績100、調整
    """
    records = []
    if sheet.nrows < 2:
        return records

    headers = [str(sheet.cell_value(0, c)).strip() for c in range(sheet.ncols)]

    col_id    = find_col(headers, "學號")
    col_mid   = find_col(headers, "期中考15", "期中考")
    col_final = find_col(headers, "期末考30", "期末考")
    col_total = find_col(headers, "總成績100", "總成績")
    col_adj   = find_col(headers, "調整")

    if col_id == -1 or col_total == -1:
        return records

    def get_trailing_cells(row_vals):
        results = []
        start = max(c for c in [col_total, col_adj] if c != -1) + 1
        for c in range(start, len(row_vals)):
            v = str(row_vals[c]).strip()
            if v and not RE_TIMESTAMP.search(v) and not re.match(r"^\d+\.?\d*$", v):
                results.append(v)
        return results

    for r in range(1, sheet.nrows):
        row_vals = [sheet.cell_value(r, c) for c in range(sheet.ncols)]

        if not is_valid_student_row(row_vals):
            continue

        sid = normalize_student_id(row_vals[col_id])

        mid_score, mid_exc   = parse_score(row_vals[col_mid]   if col_mid   != -1 else "")
        fin_score, fin_exc   = parse_score(row_vals[col_final] if col_final != -1 else "")
        tot_score, tot_exc   = parse_score(row_vals[col_total])
        adj_val,   _         = parse_score(row_vals[col_adj]   if col_adj   != -1 else "")

        exceptions = mid_exc + fin_exc + tot_exc

        class_name = None
        class_code_raw = None
        trailing = get_trailing_cells(row_vals)
        for cell_text in trailing:
            code, name = parse_class_from_cell(cell_text)
            if name and class_name is None:
                class_code_raw = code
                class_name     = name
            exc = parse_exceptions(cell_text)
            exceptions.extend(exc)

        seen = set()
        unique_exc = []
        for e in exceptions:
            key = e["tag"]
            if key not in seen:
                seen.add(key)
                unique_exc.append(e)

        record = {
            "student_id":      sid,
            "semester":        semester,
            "sheet_name":      sheet.name,
            "type":            "practicum",
            "class_code_raw":  class_code_raw,
            "class_name":      class_name or sheet.name,
            "midterm":         mid_score,
            "final":           fin_score,
            "semester_score":  tot_score,
            "adjusted":        adj_val,
            "reading_pct":     None,
            "attend_pct":      None,
            "reading_hours":   None,
            "exceptions":      unique_exc,
            "is_retaker":      False,
            "delta":           None,
        }
        records.append(record)

    return records


# ══════════════════════════════════════════════════════════════
# 跨學期重修判定
# ══════════════════════════════════════════════════════════════

def compute_retaker_flags(all_records: list) -> list:
    """
    跨學期比對，標記重修生並計算 delta。
    重修鍵：student_id + sheet_name（正課/實驗分開計算）
    """
    # 以 (student_id, sheet_name, type) 分組，按學期排序
    groups = defaultdict(list)
    for rec in all_records:
        key = (rec["student_id"], rec["sheet_name"], rec["type"])
        groups[key].append(rec)

    for key, recs in groups.items():
        if len(recs) < 2:
            continue
        # 依學期排序
        recs_sorted = sorted(recs, key=lambda x: x["semester"])
        first_score = recs_sorted[0]["semester_score"]

        for i, rec in enumerate(recs_sorted):
            rec["is_retaker"] = True
            if i == 0:
                rec["delta"] = None  # 首修無 delta
            else:
                if rec["semester_score"] is not None and first_score is not None:
                    rec["delta"] = round(rec["semester_score"] - first_score, 2)

    return all_records


# ══════════════════════════════════════════════════════════════
# 彙整 class_summary
# ══════════════════════════════════════════════════════════════

def compute_class_summary(all_records: list) -> dict:
    """計算各學期各班的統計摘要"""
    groups = defaultdict(list)
    for rec in all_records:
        key = f"{rec['semester']}_{rec['sheet_name']}"
        groups[key].append(rec)

    summary = {}
    for key, recs in groups.items():
        scores = [r["semester_score"] for r in recs if r["semester_score"] is not None]
        mids   = [r["midterm"]        for r in recs if r["midterm"]        is not None]
        fins   = [r["final"]          for r in recs if r["final"]          is not None]
        n = len(scores)

        # 分數分佈（10分一組，0-9, 10-19, ..., 90-100）
        dist = [0] * 11
        for s in scores:
            bucket = min(int(s // 10), 10)
            dist[bucket] += 1

        pass_count    = sum(1 for s in scores if s >= 60)
        retaker_count = sum(1 for r in recs if r["is_retaker"])

        summary[key] = {
            "semester":      recs[0]["semester"],
            "sheet_name":    recs[0]["sheet_name"],
            "type":          recs[0]["type"],
            "count":         len(recs),
            "avg_midterm":   round(sum(mids)/len(mids), 2) if mids else None,
            "avg_final":     round(sum(fins)/len(fins), 2) if fins else None,
            "avg_semester":  round(sum(scores)/n, 2)      if scores else None,
            "pass_rate":     round(pass_count/n, 4)       if n else None,
            "fail_rate":     round((n-pass_count)/n, 4)   if n else None,
            "retaker_rate":  round(retaker_count/n, 4)    if n else None,
            "score_distribution": dist,
        }

    return summary


# ══════════════════════════════════════════════════════════════
# 彙整 students（以學號為 key）
# ══════════════════════════════════════════════════════════════

def build_student_map(all_records: list) -> dict:
    """以遮蔽學號為 key，整合所有修課記錄"""
    students = defaultdict(lambda: {"name_masked": "", "records": []})

    for rec in all_records:
        sid = rec["student_id"]
        masked = mask_student_id(sid)
        students[sid]["name_masked"] = masked

        entry = {
            "semester":       rec["semester"],
            "sheet_name":     rec["sheet_name"],
            "type":           rec["type"],
            "class_code_raw": rec["class_code_raw"],
            "class_name":     rec["class_name"],
            "midterm":        rec["midterm"],
            "final":          rec["final"],
            "semester_score": rec["semester_score"],
            "adjusted":       rec["adjusted"],
            "reading_pct":    rec["reading_pct"],
            "attend_pct":     rec["attend_pct"],
            "reading_hours":  rec["reading_hours"],
            "is_retaker":     rec["is_retaker"],
            "delta":          rec["delta"],
            "exceptions":     rec["exceptions"],
        }
        students[sid]["records"].append(entry)

    # 每位學生的記錄依學期排序
    for sid, data in students.items():
        data["records"].sort(key=lambda x: (x["semester"], x["sheet_name"]))

    return dict(students)


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════

def run_etl(raw_dir: str, out_path: str):
    print(f"[ETL] 掃描資料夾：{raw_dir}")

    xls_files = sorted([
        os.path.join(raw_dir, f)
        for f in os.listdir(raw_dir)
        if f.lower().endswith(".xls") or f.lower().endswith(".xlsx")
    ])

    if not xls_files:
        print("[ETL] ⚠️  未找到任何 .xls 檔案，請確認 raw_data/ 資料夾")
        return

    all_records = []
    semesters_seen = set()

    for fpath in xls_files:
        semester = parse_semester_from_filename(fpath)
        semesters_seen.add(semester)
        fname = os.path.basename(fpath)
        print(f"  → 處理：{fname}  (學期代碼: {semester})")

        try:
            wb = xlrd.open_workbook(fpath)
        except Exception as e:
            print(f"     ❌ 開啟失敗：{e}")
            continue

        for sh_name in wb.sheet_names():
            sh = wb.sheet_by_name(sh_name)
            sheet_type = detect_sheet_type(sh_name)

            if sheet_type == "practicum":
                recs = parse_practicum_sheet(sh, semester)
            else:
                recs = parse_theory_sheet(sh, semester, sheet_type)

            print(f"     分頁「{sh_name}」({sheet_type}): {len(recs)} 筆有效學生記錄")
            all_records.extend(recs)

    print(f"\n[ETL] 共 {len(all_records)} 筆原始記錄，開始跨學期重修判定...")
    all_records = compute_retaker_flags(all_records)

    retaker_count = sum(1 for r in all_records if r["is_retaker"])
    print(f"[ETL] 重修記錄：{retaker_count} 筆")

    print("[ETL] 計算班級統計摘要...")
    class_summary = compute_class_summary(all_records)

    print("[ETL] 建立學生資料表...")
    students = build_student_map(all_records)

    output = {
        "meta": {
            "generated_at":   datetime.now().isoformat(timespec="seconds"),
            "semesters":      sorted(semesters_seen),
            "total_students": len(students),
            "total_records":  len(all_records),
            "schema_version": "2.1",
        },
        "students":      students,
        "class_summary": class_summary,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n[ETL] ✅ 完成！輸出至：{out_path}")
    print(f"       學生數：{len(students)}")
    print(f"       班級摘要：{len(class_summary)} 個班次")
    print(f"       學期涵蓋：{sorted(semesters_seen)}")


# ══════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="學習數據分析儀表板 ETL 腳本")
    parser.add_argument("--raw_dir", default=DEFAULT_RAW_DIR,
                        help=f"原始 xls 資料夾（預設：{DEFAULT_RAW_DIR}）")
    parser.add_argument("--out",     default=DEFAULT_OUT,
                        help=f"輸出 JSON 路徑（預設：{DEFAULT_OUT}）")
    args = parser.parse_args()

    run_etl(args.raw_dir, args.out)
