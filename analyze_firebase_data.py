#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Firebase 데이터베이스에서 사용자의 체다 대화, 식단 기록 날짜 및 체중 정보를 추출하여 테이블 형태로 README.md에 저장하는 스크립트

이 스크립트는 다음 기능을 수행합니다:
1. Firebase 데이터베이스 연결
2. 사용자 이메일별 체다 대화 날짜 추출 (session > [이메일] > cheddar 문서 분석)
3. 사용자 이메일별 식단 기록 날짜 추출 (session > [이메일] > meal_tracking 문서 분석)
4. 사용자 이메일별 체중 기록 데이터 추출 (session > [이메일] > chat_ignore_weekly_data > [날짜] > weight)
5. 사용자 정보 추출 (patient > [이메일] > name)
6. 결과를 테이블 형태로 README.md 파일에 저장
"""

import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import calendar
import os
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any

class FirebaseAnalyzer:
    """Firebase 데이터베이스에서 사용자 활동 날짜와 체중 정보를 추출하는 클래스"""
    
    def __init__(self, credential_path: str) -> None:
        """
        Firebase 분석기 초기화
        
        Args:
            credential_path: Firebase 인증 정보가 담긴 JSON 파일 경로
        """
        self.cred = credentials.Certificate(credential_path)
        firebase_admin.initialize_app(self.cred)
        self.db = firestore.client()
        self.user_emails = [
            "lsm040117@gmail.com",
            "ddocdk77@gmail.com",
            "qqyer6953@naver.com",
            "wonhyeonseo62@gmail.com",
            "asnox07@naver.com",
            "081212hy@gmail.con",
            "elprup135@gmail.com",
            "tlsworua821@gmail.com",

            "ijeongyeon199@gmail.com",
            "cjh060914@gmail.com",
            "a01056848676@gmail.com",
            "ppulyon9@gmail.com",
            "heb75707@gmail.com",
            "eg13111@gmail.com",
            "namgiseong8@gmail.com",
            "aa01094102563@gmail.com",
            "a01093053968@gmail.com",
            "juhyen1221@naver.com",
            "jedidiah0219@gmail.com",
            "chgml080925@hanmail.net",

            "bugae48@gmail.com",
            "qkrgkdus6104@gmail.com",
            "park21770420@icloud.com",
            "55413175a@gmail.com",
            "amchiyongjh@gmail.com",
            "haerinshin@naver.com",
            "imsarang4825@gmail.com",

            "01086506058@naver.com",
            "kimjiho1079@naver.com",
            "01082792251@naver.com",
            "two0329@naver.com",
            "01076733914@naver.com",
        ]
        self.user_names = {}  # 사용자 이메일별 이름 저장
        
    def extract_date_from_document_id(self, doc_id: str) -> Optional[datetime.date]:
        """
        문서 ID에서 날짜 정보 추출 (마지막 8자리에서 YYYYMMDD 형식 추출)
        
        Args:
            doc_id: Firebase 문서 ID
            
        Returns:
            추출된 날짜 객체 또는 None
        """
        try:
            date_str = doc_id[-8:]  # 문서 ID의 마지막 8자리
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return datetime.date(year, month, day)
        except (ValueError, IndexError):
            # 날짜 형식이 아닌 경우 None 반환
            return None
    
    def get_user_name(self, email: str) -> str:
        """
        환자 컬렉션에서 사용자 이름 가져오기
        
        Args:
            email: 사용자 이메일
            
        Returns:
            사용자 이름 또는 이메일 ID
        """
        try:
            name_doc = self.db.collection('patient').document(email).get()
            if name_doc.exists and 'name' in name_doc.to_dict():
                return name_doc.to_dict()['name']
            return email.split('@')[0]  # 이름이 없으면 이메일 ID 반환
        except Exception as e:
            print(f"사용자 이름 가져오기 오류 ({email}): {e}")
            return email.split('@')[0]
    
    def get_cheddar_conversation_dates(self, email: str) -> Set[datetime.date]:
        """
        특정 사용자의 체다 대화 날짜 추출
        
        Args:
            email: 사용자 이메일
            
        Returns:
            체다 대화 날짜 집합
        """
        cheddar_dates = set()
        cheddar_ref = self.db.collection('session').document(email).collection('cheddar')
        
        try:
            docs = cheddar_ref.stream()
            for doc in docs:
                date = self.extract_date_from_document_id(doc.id)
                if date:
                    cheddar_dates.add(date)
        except Exception as e:
            print(f"체다 대화 날짜 추출 오류 ({email}): {e}")
            
        return cheddar_dates
    
    def get_meal_tracking_dates(self, email: str) -> Set[datetime.date]:
        """
        특정 사용자의 식단 기록 날짜 추출
        
        Args:
            email: 사용자 이메일
            
        Returns:
            식단 기록 날짜 집합
        """
        meal_dates = set()
        meal_ref = self.db.collection('session').document(email).collection('meal_tracking')
        
        try:
            docs = meal_ref.stream()
            for doc in docs:
                date = self.extract_date_from_document_id(doc.id)
                if date:
                    meal_dates.add(date)
        except Exception as e:
            print(f"식단 기록 날짜 추출 오류 ({email}): {e}")
            
        return meal_dates
    
    def get_weight_data(self, email: str) -> Dict[datetime.date, float]:
        """
        특정 사용자의 체중 기록 추출
        
        Args:
            email: 사용자 이메일
            
        Returns:
            날짜별 체중 데이터 딕셔너리
        """
        weight_data = {}
        weekly_data_ref = self.db.collection('session').document(email).collection('chat_ignore_weekly_data')
        
        try:
            docs = weekly_data_ref.stream()
            for doc in docs:
                date = self.extract_date_from_document_id(doc.id)
                if date and 'weight' in doc.to_dict():
                    weight = doc.to_dict()['weight']
                    if isinstance(weight, (int, float)) and weight > 0:
                        weight_data[date] = float(weight)
        except Exception as e:
            print(f"체중 데이터 추출 오류 ({email}): {e}")
            
        return weight_data
    
    def analyze_all_users(self) -> Tuple[Dict[str, Set[datetime.date]], Dict[str, Set[datetime.date]], Dict[str, Dict[datetime.date, float]], Dict[str, str]]:
        """
        모든 사용자의 체다 대화, 식단 기록 날짜 및 체중 데이터 분석
        
        Returns:
            (사용자별 체다 대화 날짜, 사용자별 식단 기록 날짜, 사용자별 체중 데이터, 사용자별 이름) 튜플
        """
        cheddar_dates_by_user = {}
        meal_dates_by_user = {}
        weight_data_by_user = {}
        user_names = {}
        
        for email in self.user_emails:
            print(f"{email} 사용자 데이터 분석 중...")
            # 사용자 이름 가져오기
            user_name = self.get_user_name(email)
            user_names[email] = user_name
            
            # 체다 대화 및 식단 기록 날짜 가져오기
            cheddar_dates = self.get_cheddar_conversation_dates(email)
            meal_dates = self.get_meal_tracking_dates(email)
            
            # 체중 데이터 가져오기
            weight_data = self.get_weight_data(email)
            
            cheddar_dates_by_user[email] = cheddar_dates
            meal_dates_by_user[email] = meal_dates
            weight_data_by_user[email] = weight_data
            
        return cheddar_dates_by_user, meal_dates_by_user, weight_data_by_user, user_names
    
    def generate_markdown_table(self, 
                               cheddar_dates_by_user: Dict[str, Set[datetime.date]], 
                               meal_dates_by_user: Dict[str, Set[datetime.date]],
                               weight_data_by_user: Dict[str, Dict[datetime.date, float]],
                               user_names: Dict[str, str]) -> str:
        """
        마크다운 테이블 생성
        
        Args:
            cheddar_dates_by_user: 사용자별 체다 대화 날짜
            meal_dates_by_user: 사용자별 식단 기록 날짜
            weight_data_by_user: 사용자별 체중 데이터
            user_names: 사용자별 이름
            
        Returns:
            마크다운 테이블 문자열
        """
        # 모든 날짜 수집 및 정렬
        all_dates = set()
        for dates in cheddar_dates_by_user.values():
            all_dates.update(dates)
        for dates in meal_dates_by_user.values():
            all_dates.update(dates)
        
        sorted_dates = sorted(all_dates)
        
        # 현재 날짜와 시간
        now = datetime.datetime.now()
        today = now.date()
        yesterday = today - datetime.timedelta(days=1)
        two_days_ago = today - datetime.timedelta(days=2)
        
        # 마크다운 테이블 헤더 생성
        markdown = "# 사용자 활동 분석\n\n"
        markdown += f"## 분석 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        markdown += "## 체다 대화 및 식단 기록 날짜\n\n"
        
        # 날짜 헤더 생성
        markdown += "| 사용자 | 이메일 |"
        for date in sorted_dates:
            markdown += f" {date.strftime('%Y-%m-%d')} |"
        markdown += "\n"
        
        # 테이블 구분선
        markdown += "|" + "---|" * (len(sorted_dates) + 2) + "\n"
        
        # 각 사용자별 데이터 행 추가
        for email in self.user_emails:
            email_display = email.split('@')[0]  # 이메일 아이디만 표시
            name_display = user_names.get(email, email_display)  # 사용자 이름
            markdown += f"| {name_display} | {email_display} |"
            
            for date in sorted_dates:
                cell_content = ""
                if date in cheddar_dates_by_user.get(email, set()):
                    cell_content += "C"
                if date in meal_dates_by_user.get(email, set()):
                    cell_content += "M"
                
                markdown += f" {cell_content} |"
            
            markdown += "\n"
        
        # 범례 추가
        markdown += "\n**범례**: C = 체다 대화, M = 식단 기록\n\n"
        
        # 체중 데이터 테이블
        markdown += "## 사용자별 체중 변화 기록\n\n"
        
        # 체중 데이터가 있는 사용자 필터링
        users_with_weight_data = [email for email in self.user_emails if weight_data_by_user.get(email)]
        
        if users_with_weight_data:
            for email in users_with_weight_data:
                name_display = user_names.get(email, email.split('@')[0])
                weight_data = weight_data_by_user.get(email, {})
                
                if weight_data:
                    markdown += f"### {name_display}의 체중 기록\n\n"
                    markdown += "| 날짜 | 체중(kg) |\n"
                    markdown += "|---|---:|\n"
                    
                    # 날짜별로 정렬
                    sorted_weight_dates = sorted(weight_data.keys(), reverse=True)
                    
                    for date in sorted_weight_dates:
                        weight = weight_data[date]
                        markdown += f"| {date.strftime('%Y-%m-%d')} | {weight:.1f} |\n"
                    
                    markdown += "\n"
        else:
            markdown += "체중 기록이 있는 사용자가 없습니다.\n\n"
        
        # 최근 사용 기록 요약
        markdown += "## 최근 사용 기록 요약\n\n"
        
        # 오늘 사용자
        markdown += "### 오늘 사용 기록\n\n"
        markdown += "| 사용자 | 사용 유형 |\n"
        markdown += "|---|---|\n"
        today_users_found = False
        for email in self.user_emails:
            name_display = user_names.get(email, email.split('@')[0])
            today_activities = []
            
            if today in cheddar_dates_by_user.get(email, set()):
                today_activities.append("체다 대화")
            if today in meal_dates_by_user.get(email, set()):
                today_activities.append("식단 기록")
                
            if today_activities:
                today_users_found = True
                markdown += f"| {name_display} | {', '.join(today_activities)} |\n"
        
        if not today_users_found:
            markdown += "| - | 오늘 사용 기록이 없습니다 |\n"
            
        # 어제 사용자
        markdown += "\n### 어제 사용 기록\n\n"
        markdown += "| 사용자 | 사용 유형 |\n"
        markdown += "|---|---|\n"
        yesterday_users_found = False
        for email in self.user_emails:
            name_display = user_names.get(email, email.split('@')[0])
            yesterday_activities = []
            
            if yesterday in cheddar_dates_by_user.get(email, set()):
                yesterday_activities.append("체다 대화")
            if yesterday in meal_dates_by_user.get(email, set()):
                yesterday_activities.append("식단 기록")
                
            if yesterday_activities:
                yesterday_users_found = True
                markdown += f"| {name_display} | {', '.join(yesterday_activities)} |\n"
        
        if not yesterday_users_found:
            markdown += "| - | 어제 사용 기록이 없습니다 |\n"
            
        # 2일 전 사용자
        markdown += "\n### 2일 전 사용 기록\n\n"
        markdown += "| 사용자 | 사용 유형 |\n"
        markdown += "|---|---|\n"
        two_days_ago_users_found = False
        for email in self.user_emails:
            name_display = user_names.get(email, email.split('@')[0])
            two_days_ago_activities = []
            
            if two_days_ago in cheddar_dates_by_user.get(email, set()):
                two_days_ago_activities.append("체다 대화")
            if two_days_ago in meal_dates_by_user.get(email, set()):
                two_days_ago_activities.append("식단 기록")
                
            if two_days_ago_activities:
                two_days_ago_users_found = True
                markdown += f"| {name_display} | {', '.join(two_days_ago_activities)} |\n"
        
        if not two_days_ago_users_found:
            markdown += "| - | 2일 전 사용 기록이 없습니다 |\n"
            
        # 사용자별 최근 활동
        markdown += "\n### 사용자별 최근 사용 기록\n\n"
        markdown += "| 사용자 | 최근 체다 대화 | 최근 식단 기록 | 최근 체중 기록 |\n"
        markdown += "|---|---|---|---|\n"
        
        for email in self.user_emails:
            name_display = user_names.get(email, email.split('@')[0])
            
            # 최근 체다 대화 날짜
            cheddar_dates = cheddar_dates_by_user.get(email, set())
            latest_cheddar = max(cheddar_dates) if cheddar_dates else None
            cheddar_display = latest_cheddar.strftime('%Y-%m-%d') if latest_cheddar else "없음"
            
            # 최근 식단 기록 날짜
            meal_dates = meal_dates_by_user.get(email, set())
            latest_meal = max(meal_dates) if meal_dates else None
            meal_display = latest_meal.strftime('%Y-%m-%d') if latest_meal else "없음"
            
            # 최근 체중 기록 날짜 및 값
            weight_dates = list(weight_data_by_user.get(email, {}).keys())
            latest_weight_date = max(weight_dates) if weight_dates else None
            latest_weight = weight_data_by_user.get(email, {}).get(latest_weight_date) if latest_weight_date else None
            
            if latest_weight_date and latest_weight:
                weight_display = f"{latest_weight_date.strftime('%Y-%m-%d')} ({latest_weight:.1f}kg)"
            else:
                weight_display = "없음"
            
            markdown += f"| {name_display} | {cheddar_display} | {meal_display} | {weight_display} |\n"
        
        return markdown
    
    def save_to_readme(self, markdown_content: str) -> None:
        """
        마크다운 내용을 README.md 파일에 저장
        
        Args:
            markdown_content: 저장할 마크다운 내용
        """
        with open("README.md", "w", encoding="utf-8") as f:
            f.write(markdown_content)
        print("분석 결과가 README.md 파일에 저장되었습니다.")


def main():
    """메인 함수"""
    # Firebase 인증 정보 경로 설정
    credential_path = os.path.join(os.path.dirname(__file__), "haram-mindfuldiary1-firebase-adminsdk-zm9xb-c4eb736168.json")

    # 인증 정보 파일 존재 확인
    if not os.path.exists(credential_path):
        print(f"Firebase 인증 정보 파일이 없습니다: {credential_path}")
        print("인증 정보 파일 경로를 올바르게 설정해주세요.")
        return
    
    # Firebase 분석기 생성 및 실행
    analyzer = FirebaseAnalyzer(credential_path)
    cheddar_dates, meal_dates, weight_data, user_names = analyzer.analyze_all_users()
    
    # 마크다운 테이블 생성 및 저장
    markdown = analyzer.generate_markdown_table(cheddar_dates, meal_dates, weight_data, user_names)
    analyzer.save_to_readme(markdown)


if __name__ == "__main__":
    main() 