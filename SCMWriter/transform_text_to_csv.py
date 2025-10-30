#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
=============================================================================
transform_text_to_csv.py - 텍스트 개인정보 비식별화 및 가명처리 도구
=============================================================================

## 목적
.txt 파일에서 개인정보가 될 수 있는 고유명사를 비식별화(가명처리)하여
원본 텍스트의 구조와 내용은 보존하되, 민감정보만 영문 대문자로 치환합니다.

## 주요 기능
1. **비식별화 대상 지정**: 사용자가 직접 개인정보 키워드 입력
2. **자동 가명 생성**: 각 고유명사를 영문 대문자(A, B, C...)로 순차 치환
3. **숫자 비식별화**: 문자열 형태의 숫자 정보도 비식별화 (NUM_1, NUM_2...)
4. **원형 보존**: 비식별화 대상 외 모든 내용 원형 유지

## 출력 결과
1. **비식별화된 텍스트 파일** (.txt): 개인정보가 가명처리된 전체 텍스트
2. **매핑 정보 파일** (.csv): 원본 → 가명 매핑 테이블

## 사용 예시

python transform_text_to_csv.py

실행 후 대화형 인터페이스에서:
1. 파일 경로 입력 (예: C:/Users/sample.txt)
2. 비식별화 대상 입력 (예: 민지,예은,김철수)
3. 숫자 비식별화 여부 선택 (Y/N)

## 출력 파일
- 비식별화된 텍스트: `<원본파일명>_anonymized_<timestamp>.txt` (원본과 같은 경로)
- 매핑 정보: `<timestamp>.csv` (원본과 같은 경로)

## CSV 컬럼
- original: 원본 단어/숫자
- anonymized: 가명 처리된 값 (A, B, C... 또는 NUM_1, NUM_2...)
- type: keyword 또는 numeric
- occurrences: 원본 텍스트에서 등장 횟수

=============================================================================
"""

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Iterator, Tuple


# =============================================================================
# 파일 탐색 함수
# =============================================================================

def iter_txt_files(path: Path) -> Iterator[Path]:
    """
    입력 경로에서 .txt 파일들을 재귀적으로 탐색합니다.
    
    Args:
        path: 파일 또는 디렉토리 경로
        
    Yields:
        .txt 파일 경로
    """
    if path.is_file():
        if path.suffix.lower() == '.txt':
            yield path
    elif path.is_dir():
        for txt_file in path.rglob('*.txt'):
            if txt_file.is_file():
                yield txt_file


# =============================================================================
# 가명 생성 함수
# =============================================================================

def generate_pseudonym(index: int, prefix: str = '') -> str:
    """
    인덱스를 기반으로 가명을 생성합니다.
    0-25: A-Z
    26-51: AA-AZ
    52-77: BA-BZ
    ...
    
    Args:
        index: 순서 인덱스
        prefix: 접두사 (예: 'NUM_')
        
    Returns:
        생성된 가명
    """
    if prefix:
        return f"{prefix}{index + 1}"
    
    # 알파벳 대문자로 변환
    result = ''
    while True:
        result = chr(65 + (index % 26)) + result
        index = index // 26
        if index == 0:
            break
        index -= 1
    return result


# =============================================================================
# 숫자 추출 함수
# =============================================================================

def extract_all_numbers(text: str) -> List[Tuple[str, int, int]]:
    """
    텍스트에서 모든 숫자 패턴을 추출합니다.
    시간 패턴(HH:MM 형식)은 제외됩니다.
    
    Args:
        text: 입력 텍스트
        
    Returns:
        [(숫자_문자열, 시작_인덱스, 종료_인덱스), ...] 리스트
    """
    # 시간 패턴 찾기 (HH:MM 형식) - 비식별화에서 제외할 위치들
    time_pattern = re.compile(r'\d{1,2}:\d{2}')
    time_spans = set()
    for match in time_pattern.finditer(text):
        # 시간 패턴에 포함된 모든 인덱스를 제외 리스트에 추가
        for i in range(match.start(), match.end()):
            time_spans.add(i)
    
    # 숫자 패턴: 콤마, 소수점 포함
    number_pattern = re.compile(r'(?<![A-Za-z0-9_])(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?(?![A-Za-z0-9_])')
    
    matches = []
    for match in number_pattern.finditer(text):
        # 시간 패턴과 겹치는 숫자는 제외 (매치 범위가 시간 패턴과 겹치는지 확인)
        is_in_time_pattern = False
        for i in range(match.start(), match.end()):
            if i in time_spans:
                is_in_time_pattern = True
                break
        
        if not is_in_time_pattern:
            matches.append((match.group(), match.start(), match.end()))
    return matches


# =============================================================================
# 비식별화 처리 함수
# =============================================================================

def create_anonymization_mapping(
    targets: List[str],
    text: str,
    case_insensitive: bool,
    anonymize_numbers: bool
) -> Tuple[Dict[str, str], Dict[str, int]]:
    """
    비식별화 매핑을 생성합니다.
    
    Args:
        targets: 비식별화 대상 리스트
        text: 원본 텍스트
        case_insensitive: 대소문자 무시 여부
        anonymize_numbers: 숫자 비식별화 여부
        
    Returns:
        (매핑_딕셔너리, 등장횟수_딕셔너리)
    """
    mapping = {}
    occurrences = {}
    index = 0
    
    # 키워드 비식별화 매핑 생성
    for target in targets:
        if target not in mapping:
            # 대소문자 무시 모드면 소문자로 통일
            key = target.lower() if case_insensitive else target
            
            # 실제 텍스트에서 등장 횟수 계산
            if case_insensitive:
                pattern = re.compile(re.escape(target), re.IGNORECASE)
            else:
                pattern = re.compile(re.escape(target))
            
            count = len(pattern.findall(text))
            
            if count > 0:  # 실제로 텍스트에 존재하는 경우만 매핑
                pseudonym = generate_pseudonym(index)
                mapping[key] = pseudonym
                occurrences[key] = count
                index += 1
    
    # 숫자 비식별화 매핑 생성
    if anonymize_numbers:
        number_matches = extract_all_numbers(text)
        unique_numbers = sorted(set(match[0] for match in number_matches))
        
        for num_idx, number in enumerate(unique_numbers):
            pseudonym = generate_pseudonym(num_idx, prefix='NUM_')
            mapping[number] = pseudonym
            # 숫자 등장 횟수 계산
            occurrences[number] = sum(1 for m in number_matches if m[0] == number)
    
    return mapping, occurrences


def anonymize_text(
    text: str,
    mapping: Dict[str, str],
    case_insensitive: bool
) -> str:
    """
    텍스트를 비식별화합니다.
    시간 패턴(HH:MM 형식)은 보호됩니다.
    
    Args:
        text: 원본 텍스트
        mapping: 비식별화 매핑
        case_insensitive: 대소문자 무시 여부
        
    Returns:
        비식별화된 텍스트
    """
    # 1단계: 시간 패턴을 임시 플레이스홀더로 보호
    time_pattern = re.compile(r'\d{1,2}:\d{2}')
    time_matches = []
    placeholder_prefix = '§§§TIME'
    placeholder_suffix = 'PROTECTED§§§'
    
    def time_replacer(match):
        idx = len(time_matches)
        time_matches.append(match.group())
        # 알파벳 인덱스 사용 (숫자 비식별화 회피)
        alpha_idx = chr(65 + idx) if idx < 26 else f"X{idx}"
        return f'{placeholder_prefix}{alpha_idx}{placeholder_suffix}'
    
    result = time_pattern.sub(time_replacer, text)
    
    # 2단계: 비식별화 처리 (긴 키워드부터 치환)
    sorted_keys = sorted(mapping.keys(), key=len, reverse=True)
    
    for original in sorted_keys:
        pseudonym = mapping[original]
        
        if case_insensitive:
            # 대소문자 무시: 정규식 플래그 사용
            pattern = re.compile(re.escape(original), re.IGNORECASE)
            result = pattern.sub(pseudonym, result)
        else:
            # 대소문자 구분: 단순 치환
            result = result.replace(original, pseudonym)
    
    # 3단계: 시간 패턴 복원
    for idx, original_time in enumerate(time_matches):
        alpha_idx = chr(65 + idx) if idx < 26 else f"X{idx}"
        placeholder = f'{placeholder_prefix}{alpha_idx}{placeholder_suffix}'
        result = result.replace(placeholder, original_time)
    
    return result


# =============================================================================
# CSV 저장 함수
# =============================================================================

def write_mapping_csv(
    mapping: Dict[str, str],
    occurrences: Dict[str, int],
    out_path: Path
) -> None:
    """
    비식별화 매핑을 CSV 파일로 저장합니다.
    
    Args:
        mapping: 비식별화 매핑
        occurrences: 등장 횟수
        out_path: 출력 CSV 파일 경로
    """
    rows = []
    for original, anonymized in mapping.items():
        # 숫자인지 키워드인지 판단
        is_numeric = anonymized.startswith('NUM_')
        rows.append({
            'original': original,
            'anonymized': anonymized,
            'type': 'numeric' if is_numeric else 'keyword',
            'occurrences': occurrences.get(original, 0)
        })
    
    # 가명 알파벳 순으로 정렬
    rows.sort(key=lambda x: x['anonymized'])
    
    fieldnames = ['original', 'anonymized', 'type', 'occurrences']
    
    with open(out_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# =============================================================================
# 메인 함수
# =============================================================================

def main():
    """
    메인 실행 함수 - 대화형 인터페이스
    """
    # 전체 예외 처리
    try:
        print("=" * 70)
        print("텍스트 개인정보 비식별화 도구")
        print("=" * 70)
        print()
        
        # =================================================================
        # 1. 파일 경로 입력
        # =================================================================
        print("📁 비식별화할 파일의 경로와 파일명을 알려주세요")
        print("   (예시: C:/Users/sample.txt 또는 C:\\Users\\sample.txt)")
        input_path_str = input(">> ").strip()
        
        if not input_path_str:
            print("❌ 파일 경로가 입력되지 않았습니다.")
            return
        
        input_path = Path(input_path_str)
        if not input_path.exists():
            print(f"❌ 파일을 찾을 수 없습니다: {input_path}")
            return
        
        txt_files = list(iter_txt_files(input_path))
        if not txt_files:
            print("❌ .txt 파일을 찾을 수 없습니다.")
            return
        
        print(f"✓ 발견된 .txt 파일: {len(txt_files)}개")
        print()
        
        # =================================================================
        # 2. 비식별화 대상 입력
        # =================================================================
        print("🔒 비식별화할 개인정보를 작성해주세요")
        print("   (여러 개일 경우 쉼표(,)로 구분. 예: 김철수,이영희,박민수)")
        targets_str = input(">> ").strip()
        
        targets = []
        if targets_str:
            targets = [t.strip() for t in targets_str.split(',') if t.strip()]
            targets = list(dict.fromkeys(targets))  # 중복 제거
            print(f"✓ 비식별화 대상: {len(targets)}개 ({', '.join(targets)})")
        else:
            print("⚠ 비식별화 대상이 입력되지 않았습니다.")
        print()
        
        # =================================================================
        # 3. 숫자 비식별화 여부
        # =================================================================
        print("🔢 숫자 정보도 비식별화하시겠습니까? (시간 패턴은 제외됩니다)")
        print("   (Y/N, 기본값: Y)")
        anonymize_numbers_input = input(">> ").strip().upper()
        anonymize_numbers = anonymize_numbers_input != 'N'
        
        if anonymize_numbers:
            print("✓ 숫자 비식별화: 활성화 (시간 패턴 HH:MM 제외)")
        else:
            print("✓ 숫자 비식별화: 비활성화")
        print()
        
        if not targets and not anonymize_numbers:
            print("❌ 비식별화할 대상이 없습니다.")
            return
        
        # =================================================================
        # 비식별화 처리 시작
        # =================================================================
        print("=" * 70)
        print("🔄 비식별화 처리 중...")
        print("=" * 70)
        print()
        
        case_insensitive = True  # 기본값: 대소문자 무시
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        total_keywords_anonymized = 0
        total_numbers_anonymized = 0
        
        # 전체 매핑 정보 (모든 파일 통합)
        global_mapping = {}
        global_occurrences = {}
        
        # 생성된 파일 경로 저장
        created_txt_files = []
        created_csv_file = None
        
        for txt_file in txt_files:
            try:
                # 파일 읽기
                with open(txt_file, 'r', encoding='utf-8') as f:
                    original_text = f.read()
                
                # 비식별화 매핑 생성
                mapping, occurrences = create_anonymization_mapping(
                    targets,
                    original_text,
                    case_insensitive,
                    anonymize_numbers
                )
                
                # 전역 매핑에 누적
                for key, value in mapping.items():
                    if key not in global_mapping:
                        global_mapping[key] = value
                        global_occurrences[key] = occurrences[key]
                    else:
                        global_occurrences[key] += occurrences[key]
                
                if not mapping:
                    print(f"⚠ {txt_file.name}: 비식별화할 대상이 없습니다.")
                    continue
                
                # 키워드/숫자 카운트
                keyword_count = sum(1 for k, v in mapping.items() if not v.startswith('NUM_'))
                number_count = sum(1 for k, v in mapping.items() if v.startswith('NUM_'))
                total_keywords_anonymized += keyword_count
                total_numbers_anonymized += number_count
                
                # 텍스트 비식별화
                anonymized_text = anonymize_text(
                    original_text,
                    mapping,
                    case_insensitive
                )
                
                # 비식별화된 텍스트 저장 (원본과 같은 경로에)
                output_filename = f"{txt_file.stem}_anonymized_{timestamp}.txt"
                output_path = txt_file.parent / output_filename
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(anonymized_text)
                
                created_txt_files.append(output_path)
                print(f"✓ {txt_file.name}: 키워드 {keyword_count}개, 숫자 {number_count}개 비식별화 완료")
            
            except Exception as e:
                print(f"⚠ {txt_file.name} 처리 중 오류: {str(e)}")
        
        # =================================================================
        # 전체 매핑 정보 CSV 저장 (원본과 같은 경로에)
        # =================================================================
        if global_mapping:
            csv_filename = f'{timestamp}.csv'
            # 첫 번째 파일과 같은 경로에 저장
            csv_path = txt_files[0].parent / csv_filename
            
            write_mapping_csv(global_mapping, global_occurrences, csv_path)
            created_csv_file = csv_path
        
        # =================================================================
        # 완료 안내
        # =================================================================
        print()
        print("=" * 70)
        print("✅ 비식별화 완료!")
        print("=" * 70)
        print()
        
        if global_mapping:
            print("📊 비식별화 요약:")
            print(f"   • 키워드 비식별화: {total_keywords_anonymized}개")
            print(f"   • 숫자 비식별화: {total_numbers_anonymized}개")
            print(f"   • 총 매핑 항목: {len(global_mapping)}개")
            print()
            
            print("📁 생성된 파일:")
            for txt_path in created_txt_files:
                print(f"   • 비식별화된 텍스트: {txt_path.absolute()}")
            if created_csv_file:
                print(f"   • 매핑 정보 CSV: {created_csv_file.absolute()}")
            print()
            
            print("💡 매핑 정보 CSV 파일을 확인하여 원본 ↔ 가명 관계를 파악할 수 있습니다.")
        else:
            print("변환할 단어가 없습니다.")
    
    except Exception as e:
        # 최상위 예외 처리
        print("데이터 오류가 발생했습니다.")
        print(f"상세: {type(e).__name__}: {str(e)}")


# =============================================================================
# 실행
# =============================================================================

if __name__ == '__main__':
    main()
