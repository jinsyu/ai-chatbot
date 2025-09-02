import pandas as pd
import numpy as np
from datetime import datetime

# 엑셀 파일 읽기
file_path = '/Users/jinsyu/Downloads/매출분석레포트_ZSDR0340.XLSX'

# 파일 정보 확인
xl_file = pd.ExcelFile(file_path)
print("시트 목록:", xl_file.sheet_names)

# 첫 번째 시트 읽기
df = pd.read_excel(file_path, sheet_name=0)

print("\n=== 데이터 정보 ===")
print(f"행 수: {len(df)}")
print(f"열 수: {len(df.columns)}")
print(f"\n컬럼 목록: {df.columns.tolist()}")

print("\n=== 데이터 타입 ===")
print(df.dtypes)

print("\n=== 첫 5행 데이터 ===")
print(df.head())

print("\n=== 데이터 통계 ===")
print(df.describe())

print("\n=== NULL 값 확인 ===")
print(df.isnull().sum())

print("\n=== 유니크 값 개수 (카테고리 컬럼) ===")
for col in df.columns:
    if df[col].dtype == 'object':
        unique_count = df[col].nunique()
        if unique_count < 50:  # 50개 미만일 때만 출력
            print(f"{col}: {unique_count}개")