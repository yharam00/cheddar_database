#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
체다 대화 식단 기록 Excel 파일에서 명단 추출하는 스크립트

요구사항:
1. 첫 사용 이후 4주가 넘은 명단 (onboarding 기간 제외)
2. 최근 2주간 체중 f/u 안된 명단
3. 한번도 체중 f/u 안된 명단

사용법:
python3 체다_명단_추출_최종.py [Excel파일명]

작성자: AI Assistant
날짜: 2025-06-16
"""

import pandas as pd
from datetime import datetime, timedelta
import re
import sys
import os

def extract_member_lists(excel_file_path):
    """
    체다 Excel 파일에서 지정된 명단을 추출하는 함수
    """
    
    if not os.path.exists(excel_file_path):
        print(f"파일을 찾을 수 없습니다: {excel_file_path}")
        return None
    
    try:
        # Excel 파일 읽기
        df = pd.read_excel(excel_file_path, header=0)
        print(f"Excel 파일 로드 완료: {df.shape[0]}행 {df.shape[1]}열")
        
        # 첫 번째 행에 날짜 정보가 있고, 두 번째 행부터 실제 데이터
        date_row = df.iloc[0]  # 날짜 정보
        data_rows = df.iloc[1:]  # 회원 데이터
        
        # 컬럼 정보
        name_col = df.columns[0]  # 이름
        signup_col = df.columns[1]  # 회원가입일
        weight_col = df.columns[2]  # 초기체중
        date_columns = df.columns[3:]  # 날짜 컬럼들
        
        # 현재 날짜 기준 계산
        today = datetime.now()
        two_weeks_ago = today - timedelta(days=14)
        
        print(f"분석 기준일: {today.strftime('%Y-%m-%d')}")
        print(f"2주 전 기준: {two_weeks_ago.strftime('%Y-%m-%d')}")
        
        # 날짜 매핑 생성
        date_mapping = create_date_mapping(date_row, date_columns)
        print(f"분석 가능한 날짜: {len(date_mapping)}개")
        
        # 결과 저장용 리스트
        over_4weeks = []
        no_weight_2weeks = []
        never_weight = []
        
        # 각 회원 분석
        valid_members = 0
        for idx, row in data_rows.iterrows():
            member_name = row[name_col]
            signup_date_str = row[signup_col]
            
            # 유효한 회원인지 확인
            if not is_valid_member(member_name):
                continue
            
            valid_members += 1
            signup_date = parse_date_string(signup_date_str)
            
            if signup_date is None:
                print(f"⚠️  {member_name}: 회원가입일 파싱 실패 ({signup_date_str})")
                continue
            
            # 1. 4주 이상 사용자 확인
            days_since_signup = (today - signup_date).days
            onboarding_days = calculate_onboarding_days(signup_date)
            effective_days = days_since_signup - onboarding_days
            
            if effective_days > 28:  # 4주 = 28일
                over_4weeks.append(member_name)
            
            # 2. 체중 기록 분석
            weight_status = analyze_weight_records(row, date_columns, date_mapping, two_weeks_ago)
            
            if not weight_status['has_any_weight']:
                never_weight.append(member_name)
            elif not weight_status['has_recent_weight']:
                no_weight_2weeks.append(member_name)
        
        print(f"\n분석 완료: 총 {valid_members}명의 회원 분석")
        
        # 결과 출력
        print_results(over_4weeks, no_weight_2weeks, never_weight)
        
        return {
            'over_4weeks': over_4weeks,
            'no_weight_2weeks': no_weight_2weeks,
            'never_weight': never_weight
        }
        
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {e}")
        return None

def is_valid_member(name):
    """유효한 회원인지 확인"""
    if pd.isna(name) or str(name).strip() == '':
        return False
    name_str = str(name).strip()
    return name_str != '이름' and len(name_str) > 0

def create_date_mapping(date_row, date_columns):
    """날짜 매핑 생성"""
    date_mapping = {}
    for col in date_columns:
        date_str = date_row[col]
        parsed_date = parse_date_string(date_str)
        if parsed_date:
            date_mapping[col] = parsed_date
    return date_mapping

def parse_date_string(date_str):
    """날짜 문자열을 datetime 객체로 변환"""
    if pd.isna(date_str):
        return None
    
    try:
        date_str = str(date_str).strip()
        
        # "03월 31일" 형태 파싱
        if "월" in date_str and "일" in date_str:
            match = re.search(r'(\d+)월\s*(\d+)일', date_str)
            if match:
                month = int(match.group(1))
                day = int(match.group(2))
                year = datetime.now().year
                return datetime(year, month, day)
        
        # pandas로 다른 형태도 시도
        return pd.to_datetime(date_str)
        
    except:
        return None

def calculate_onboarding_days(signup_date):
    """onboarding 기간 계산 (배포 첫 주 제외)"""
    weekday = signup_date.weekday()  # 0=월요일, 6=일요일
    
    # 목요일 배포 가정: 해당 주 일요일까지가 onboarding 기간
    days_to_sunday = (6 - weekday) % 7
    if days_to_sunday == 0:  # 일요일 가입
        days_to_sunday = 7
    
    return max(1, min(days_to_sunday + 1, 7))

def analyze_weight_records(row, date_columns, date_mapping, two_weeks_ago):
    """체중 기록 분석"""
    has_any_weight = False
    has_recent_weight = False
    
    for col in date_columns:
        cell_value = row[col]
        
        if pd.notna(cell_value) and is_weight_record(cell_value):
            has_any_weight = True
            
            # 최근 2주 내 기록인지 확인
            if col in date_mapping and date_mapping[col] >= two_weeks_ago:
                has_recent_weight = True
    
    return {
        'has_any_weight': has_any_weight,
        'has_recent_weight': has_recent_weight
    }

def is_weight_record(cell_value):
    """체중 기록인지 판단"""
    cell_str = str(cell_value).strip().lower()
    
    # 1. "kg"이 포함된 경우
    if 'kg' in cell_str:
        return True
    
    # 2. 숫자만 있고 체중 범위 내인 경우
    try:
        # 소수점 포함 숫자 패턴
        if re.match(r'^\d+(\.\d+)?$', cell_str):
            weight_val = float(cell_str)
            return 30 <= weight_val <= 200
    except:
        pass
    
    return False

def print_results(over_4weeks, no_weight_2weeks, never_weight):
    """결과 출력"""
    print("\n" + "="*60)
    print("📊 체다 명단 추출 결과")
    print("="*60)
    
    print(f"\n1️⃣ 첫 사용 이후 4주가 넘은 명단 ({len(over_4weeks)}명)")
    print("   (배포 첫 주 onboarding 기간 제외)")
    print("-" * 40)
    for i, name in enumerate(over_4weeks, 1):
        print(f"   {i:2d}. {name}")
    
    print(f"\n2️⃣ 최근 2주간 체중 f/u 안된 명단 ({len(no_weight_2weeks)}명)")
    print("-" * 40)
    for i, name in enumerate(no_weight_2weeks, 1):
        print(f"   {i:2d}. {name}")
    
    print(f"\n3️⃣ 한번도 체중 f/u 안된 명단 ({len(never_weight)}명)")
    print("-" * 40)
    for i, name in enumerate(never_weight, 1):
        print(f"   {i:2d}. {name}")

def save_results_to_excel(results, output_file="체다_명단_추출_결과.xlsx"):
    """결과를 Excel 파일로 저장"""
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 요약 시트
            summary_data = {
                '구분': ['4주 이상 사용자', '최근 2주간 체중기록 없음', '체중기록 전혀 없음'],
                '인원수': [len(results['over_4weeks']), 
                         len(results['no_weight_2weeks']), 
                         len(results['never_weight'])]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='요약', index=False)
            
            # 각 명단별 시트
            if results['over_4weeks']:
                pd.DataFrame({'이름': results['over_4weeks']}).to_excel(
                    writer, sheet_name='4주이상사용자', index=False)
            
            if results['no_weight_2weeks']:
                pd.DataFrame({'이름': results['no_weight_2weeks']}).to_excel(
                    writer, sheet_name='2주간체중기록없음', index=False)
            
            if results['never_weight']:
                pd.DataFrame({'이름': results['never_weight']}).to_excel(
                    writer, sheet_name='체중기록없음', index=False)
        
        print(f"\n💾 결과 저장 완료: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Excel 저장 오류: {e}")
        return False

def main():
    """메인 함수"""
    print("🍀 체다 명단 추출 프로그램")
    print("=" * 40)
    
    # 파일명 확인
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    else:
        excel_file = "체다_대화_식단_기록.xlsx"
    
    print(f"📂 분석 파일: {excel_file}")
    
    # 명단 추출 실행
    results = extract_member_lists(excel_file)
    
    if results is None:
        print("❌ 분석 실패!")
        return
    
    # 결과 저장
    output_file = f"4주_체중_명단_추출_결과_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    save_success = save_results_to_excel(results, output_file)
    
    if save_success:
        print(f"\n✅ 분석 완료!")
        print(f"📊 총 추출된 명단:")
        print(f"   - 4주 이상 사용자: {len(results['over_4weeks'])}명")
        print(f"   - 2주간 체중기록 없음: {len(results['no_weight_2weeks'])}명")
        print(f"   - 체중기록 전혀 없음: {len(results['never_weight'])}명")

if __name__ == "__main__":
    main() 