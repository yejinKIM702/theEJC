# 업데이트된 비즈니스 규칙 V2.5
## (상태머신 V2.4 및 매출확정 프로세스 V2.5 기준)

---

## 📋 개요

본 문서는 기존 비즈니스 규칙(5.1~5.4)을 **상태머신 V2.4(PO/SO/TRANSFER/RETURN)** 및 **매출확정 프로세스 V2.5** 기준으로 전면 업그레이드한 완전한 비즈니스 규칙입니다. Java 기반 의사코드와 함께 제공되어 실제 시스템 구현에 바로 적용 가능합니다.

---

## 🔄 주요 변경사항 요약

| 항목 | 기존 규칙 | V2.5 업데이트 | 사유 |
|------|---------|------------|------|
| **정상반품 기한** | 배송 시작 전만 가능 | 배송 완료 후 7일 이내 | 고객 반품기간 보장 |
| **하자반품 기한** | 배송 완료 후 7일 | 유지 (배송 완료 후 7일) | AS 기한 정책 유지 |
| **부분반품** | 미반영 | Line Item 레벨 처리 | 부분 배송·부분 반품 현실적 반영 |
| **평가절하** | 미반영 | 50% 기본값 (조정 가능) | 하자반품 재입고 시 손상도 반영 |
| **책임소재** | 미반영 | LIABILITY_DETERMINED | 회사/택배사/제조사 구분 |
| **매출확정** | 반품 미요청만 | 반품 상태 전체 확인 (REQUESTED 포함) | REQUESTED 상태도 매출확정 제외 |
| **교환 처리** | 미반영 | 기존 취소 + 신규 생성 (회계적 상쇄) | 교환 회계 명확화 |
| **배송 예외** | 미반영 | IN_TRANSIT_DELAYED/LOST/DAMAGED | 배송 예외 상황 처리 |
| **회계 전표** | 기본 전표만 | V3 CoA 규칙 (20개 계정, 6가지 거래유형) | 회계기준 완전 준수 |
| **배송 실패 재시도** | 미반영 | DELIVERY_FAILED 분기 처리 | 배송 실패 시 재배송 프로세스 |

---

## 📌 상세 비즈니스 규칙

### 5.1 정상반품 검증 규칙 (Normal Return Validation)

#### V2.5 업데이트 내용

**기존**: 배송 시작 전에만 정상반품 가능  
**V3.0**: 배송 완료 후 7일(달력일) 이내 반품 가능

#### 검증 조건

1. **배송상태 확인**
   - 배송 시작 이전 상태는 반품 불가 (PACK_READY 이전 상태)
   - 배송 시작 이후 상태만 반품 가능 (WAYBILL_GENERATED 이상)
   - 특히 배송 실패(DELIVERY_FAILED) 상태도 반품 가능

2. **반품 기한 확인**
   - 배송완료(delivered_dt) 기준 달력일로 7일 계산
   - 10일을 초과하면 반품 불가
   ```
   daysBetween = request_dt - delivered_dt (달력일)
   IF daysBetween > 10 THEN 반품 거부
   ```

3. **반품수량 검증**
   - 개별 주문 라인의 반품수량 <= 주문수량
   - 라인별 누적 반품수량 검증 (부분반품 여러 번 가능)
   - 모든 반품 시도의 누적합이 주문수량 초과 불가

4. **부분반품 처리**
   - SO_Line 레벨에서 라인별 상태 관리
   - 같은 라인에서 여러 번 반품 가능 (누적 검증)
   - 각 라인별로 반품수량 추적

#### 코드 예시 (Java)
```java
public static ReturnValidationResult validateNormalReturn(
        SO_Header so,
        LocalDateTime deliveredDt,
        LocalDateTime requestDt,
        Integer lineNo,
        Integer returnQty) {
    
    // 조건 1: 배송완료 상태 확인
    if (!isShipped(so.getStatus())) {
        return INVALID("반품은 배송 완료 이후에만 가능");
    }
    
    // 조건 2: 10일 기한 확인 (달력일 기준)
    long daysBetween = ChronoUnit.DAYS.between(
        deliveredDt.toLocalDate(),
        requestDt.toLocalDate()
    );
    if (daysBetween > 3) {
        return INVALID("배송완료 후 3일 이내만 반품 가능");
    }
    
    // 조건 3: 라인 반품수량 검증
    SO_Line soLine = so.getLineByNumber(lineNo);
    if (returnQty > soLine.getQty()) {
        return INVALID("반품수량이 주문수량 초과");
    }
    
    // 조건 4: 누적 반품수량 검증
    Integer existingReturnQty = getExistingReturnQty(so.getSoNo(), lineNo);
    if ((existingReturnQty + returnQty) > soLine.getQty()) {
        return INVALID("누적 반품수량이 주문수량 초과");
    }
    
    return VALID();
}
```

---

### 5.2 하자반품 검증 규칙 (Defect Return Validation)

#### V2.5 업데이트 내용

**기존**: 배송 완료 후 7일 이내, QC 검수 후 재입고 또는 폐기  
**V2.5**: 동일 기한 유지, 평가절하(50%) 로직 추가, 책임소재 판정 추가

#### 검증 조건

1. **배송상태 확인**
   - 배송 완료(DELIVERED) 이상 상태만 하자반품 가능
   - PARTIALLY_DELIVERED, COMPLETED 상태도 가능

2. **AS 기한 확인 (7일)**
   - 배송완료(delivered_dt) 기준 달력일로 7일 계산
   - 7일 초과 불가
   ```
   daysBetween = request_dt - delivered_dt (달력일)
   IF daysBetween > 7 THEN AS 기한 만료, 반품 거부
   ```

3. **반품수량 검증**
   - 정상반품과 동일 (주문수량 대비 검증)
   - 부분반품 누적 검증

4. **QC 검수 후 처리 분기 (V2.5 핵심)**
   - **DEFECT_CONFIRMED (실제 불량)**
     - 폐기: 100% 손실 처리 → 회계전표 (재고평가손실)
     - 재입고: 50% 평가절하 → 회계전표 (평가절하 재고 + 손실)
   - **NO_DEFECT_FOUND (불량 아님)**
     - 반품 거부 → 추가 회계처리 없음

5. **책임소재 판정 (V2.5 추가)**
   - LIABILITY_DETERMINED: 회사 / 택배사 / 제조사
   - 책임 주체에 따라 환불금 처리 주체 결정

#### 코드 예시 (Java)
```java
public static ReturnValidationResult validateDefectReturn(
        SO_Header so,
        LocalDateTime deliveredDt,
        LocalDateTime requestDt,
        Integer lineNo,
        Integer returnQty) {
    
    // 조건 1: 배송완료 상태 확인
    if (!isDelivered(so.getStatus())) {
        return INVALID("하자반품은 배송 완료 이후만 가능");
    }
    
    // 조건 2: 7일 AS 기한 확인
    long daysBetween = ChronoUnit.DAYS.between(
        deliveredDt.toLocalDate(),
        requestDt.toLocalDate()
    );
    if (daysBetween > 7) {
        return INVALID("AS 기한은 배송완료 후 7일");
    }
    
    // 조건 3, 4: 반품수량 및 누적 검증 (정상반품과 동일)
    // ...
    
    return VALID();
}

// QC 검수 후 처리 분기
public static void processDefectReturnAfterQC(QC_Result qcResult, Double damageRatio) {
    if (qcResult.getResult().equals("DEFECT_CONFIRMED")) {
        if (qcResult.isRestockYn() == false) {
            // 폐기 처리
            createJournalsForDisposal(qcResult, 100);  // 100% 손실
        } else {
            // 재입고 처리 (평가절하)
            createJournalsForRestockWithValuation(qcResult, damageRatio);  // 기본 50%
        }
    } else if (qcResult.getResult().equals("NO_DEFECT_FOUND")) {
        // 반품 거부 (추가 처리 없음)
        rejectReturn(qcResult);
    }
}
```

---

### 5.3 반품 환불 처리 및 회계 전표 생성 

#### 거래 유형별 전표 처리

##### ① 정상반품 환불 (Normal Return Refund)

**조건**: 정상반품 승인 완료

**전표 생성** (4개):
```
JE-1: Dr 매출환입(4020) / Cr 미지급금(1050)       100,000  (매출 환입)
JE-2: Dr 부가세예수금(1060) / Cr 부가세납부금(2040)  10,000  (부가세 환급)
JE-3: Dr 상품(1020) / Cr 매출원가(5010)           80,000  (재고 복구)
JE-4: Dr 반품운송료(6020) / Cr 미지급금(1050)       5,000  (배송료 부담)
```

**환불 처리**:
- 환불액: 100,000 (상품) + 10,000 (부가세) = 110,000
- 상태: REFUND_TRANSACTION 생성, 상태 = PENDING

---

##### ② 하자반품 환입 - 폐기 (Defect Return Disposal)

**조건**: 하자반품 승인 + QC DEFECT_CONFIRMED + 폐기

**전표 생성** (3개):
```
JE-1: Dr 매출환입(4020) / Cr 미지급금(1050)       100,000  (매출 환입)
JE-2: Dr 부가세예수금(1060) / Cr 부가세납부금(2040)  10,000  (부가세 환급)
JE-3: Dr 재고평가손실(6010) / Cr 매출원가(5010)    80,000  (손실 인식)
```

**환불 처리**: 정상반품과 동일

---

##### ③ 하자반품 환입 - 재입고 (Defect Return Restock with Valuation)

**조건**: 하자반품 승인 + QC DEFECT_CONFIRMED + 재입고 가능

**평가절하 계산** (기본 50%, QC에서 조정 가능):
```
손상도율(damage_ratio) = 50%  // 또는 QC.damage_ratio
복구가능액 = 80,000 × (1 - 0.5) = 40,000
손실액 = 80,000 - 40,000 = 40,000
```

**전표 생성** (4개):
```
JE-1: Dr 매출환입(4020) / Cr 미지급금(1050)           100,000  (매출 환입)
JE-2: Dr 부가세예수금(1060) / Cr 부가세납부금(2040)    10,000  (부가세 환급)
JE-3: Dr 상품(평가절하)(1030) / Cr 매출원가(5010)      40,000  (평가절하 재고)
JE-4: Dr 재고평가손실(6010) / Cr 매출원가(5010)       40,000  (손실액)
```

**환불 처리**: 정상반품과 동일

---

##### ④ 교환 (Exchange)

**조건**: 교환 승인 (type=EXCHANGE)

**처리 방식**: 기존 주문 취소 + 신규 주문 생성 (회계적 상쇄)

**전표 생성** (4개 기본 + 금액차액):
```
[기존 주문 취소]
JE-1: Dr 매출환입(4020) / Cr 미지급금(1050)           100,000  (매출 환입)
JE-2: Dr 상품(1020) / Cr 매출원가(5010)               80,000  (상품 반품)

[신규 주문 생성]
JE-3: Dr 미지급금(1050) / Cr 매출(4010)              100,000  (신규 매출)
JE-4: Dr 매출원가(5010) / Cr 상품(1020)               80,000  (신규 원가)

[금액 차이 조정] - 신규 금액 != 기존 금액인 경우
JE-5: Dr 미지급금(1050) / Cr 매출(4010) 또는 역   (차액)
```

**특징**: 금액이 같으면 회계적 상쇄 (순효과 0), 재고만 교체

---

### 5.4 매출확정 배치 (Revenue Confirmation Batch)

#### V2.5 업데이트 내용

**기존**:
- 배송 완료 후 10일 경과
- 반품 미요청 시 매출확정

**V3**:
- 배송 완료 후 7일 경과 (유지)
- **반품 상태 전체 확인** (REQUESTED 포함) ← **중요**
- 부분배송/부분반품 시 Line별 상태 확인
- 배치 재실행으로 누락 건 처리

#### 배치 로직

##### Step 1: 매출확정 대상 조회

```sql
SELECT * FROM SO_Header
WHERE status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND revenue_confirmed_dt IS NULL
  AND DATE_ADD(delivered_dt, INTERVAL 10 DAY) <= CURDATE()
  AND NOT EXISTS (
    SELECT 1 FROM Return_Header
    WHERE Return_Header.so_no = SO_Header.so_no
      AND Return_Header.status IN ('REQUESTED', 'UNDER_REVIEW', 'APPROVED')
      -- ↑ V2.5 핵심: REQUESTED도 확인!
  )
ORDER BY delivered_dt ASC
```

##### Step 2: 각 주문별 매출확정 판정

1. **배송 완료 후 7일 경과 확인**
   ```
   daysSinceDelivery = CURRENT_DATE - delivered_dt
   IF daysSinceDelivery < 7 THEN NOT CONFIRMABLE
   ```

2. **반품 상태 전체 확인**
   ```
   activeReturns = SELECT * FROM Return_Header
                   WHERE so_no = ?
                   AND status IN ('REQUESTED', 'UNDER_REVIEW', 'APPROVED')
   
   IF activeReturns.COUNT > 0 THEN NOT CONFIRMABLE
   ```

3. **완료된 반품의 회계처리 확인**
   ```
   completedReturns = SELECT * FROM Return_Header
                      WHERE so_no = ?
                      AND status IN ('CLOSED', 'COMPLETED')
   
   FOR EACH completedReturn:
       IF NOT isReturnAccountingCompleted(completedReturn) THEN
           NOT CONFIRMABLE
   ```

4. **부분배송/부분반품 시 Line별 상태 확인**
   ```
   IF so.status = 'PARTIALLY_DELIVERED' OR hasPartialReturn(so) THEN
       FOR EACH SO_Line:
           lineReturnQty = SUM(Return_Line.return_qty)
           lineDeliveredQty = SO_Line.qty
           confirmedQty = lineDeliveredQty - lineReturnQty
           
           IF confirmedQty <= 0 THEN
               SKIP (모두 반품된 라인)
   ```

##### Step 3: 매출확정 처리

**조건 만족 시**:
1. 매출확정 전표 생성 
2. SO 상태 업데이트: COMPLETED
3. revenue_confirmed_dt 기록
4. REVENUE 엔티티 생성 (별도 관리)

**조건 미충족 시**:
1. SO 상태 유지: REVENUE_PENDING
2. 다음 배치에서 재시도

##### Step 4: 배치 재실행

- 매일 자정(00:00) 실행
- 누락된 건에 대해 자동 재시도
- 모든 반품 완료 후 자동 매출확정

#### 코드 예시 (Java)
```java
public static Integer executeRevenueConfirmationBatch() {
    LocalDateTime batchExecutionTime = LocalDateTime.now();
    List<SO_Header> targetOrders = querySalesOrdersForRevenueConfirmation(batchExecutionTime);
    
    Integer processedCount = 0;
    
    for (SO_Header so : targetOrders) {
        RevenueConfirmationResult result = judgeRevenueConfirmation(so, batchExecutionTime);
        
        if (result.isConfirmable()) {
            // 매출 확정
            confirmRevenue(so, result.getJournals());
            processedCount++;
        } else {
            // 상태 유지
            updateSOStatus(so.getSoNo(), "REVENUE_PENDING");
        }
    }
    
    return processedCount;
}

private static RevenueConfirmationResult judgeRevenueConfirmation(
        SO_Header so,
        LocalDateTime batchTime) {
    
    // 조건 1: 10일 경과 확인
    long daysSinceDelivery = ChronoUnit.DAYS.between(
        so.getDeliveredDt().toLocalDate(),
        batchTime.toLocalDate()
    );
    if (daysSinceDelivery < 7) {
        return NOT_CONFIRMABLE("배송완료 후 미만 경과");
    }
    
    // 조건 2: 활성 반품 확인 (REQUESTED 포함) ← V2.5 핵심
    List<Return_Header> activeReturns = getActiveReturns(so.getSoNo());
    if (!activeReturns.isEmpty()) {
        return NOT_CONFIRMABLE("활성 반품 존재");
    }
    
    // 조건 3: 완료된 반품의 회계처리 확인
    List<Return_Header> completedReturns = getCompletedReturns(so.getSoNo());
    for (Return_Header returnHeader : completedReturns) {
        if (!isReturnAccountingCompleted(returnHeader)) {
            return NOT_CONFIRMABLE("반품 회계처리 미완료");
        }
    }
    
    // 조건 4: Line별 상태 확인
    // ...
    
    return CONFIRMABLE(generateJournals(so));
}
```

---

## 🔑 주요 포인트 정리

### V2.5 핵심 변경사항

1. **정상반품 기한 확대**
   - 배송 시작 전만 가능 → 배송 완료 후 7일 이내
   - 고객 보호 강화

2. **부분반품/부분배송 지원**
   - SO_Line 레벨 상태 관리
   - 각 라인별 반품수량 추적

3. **평가절하 로직**
   - 하자반품 재입고 시 50% 기본 평가절하
   - QC에서 조정 가능

4. **책임소재 명확화**
   - LIABILITY_DETERMINED 필드 추가
   - 회사/택배사/제조사 구분

5. **매출확정 배치 강화**
   - REQUESTED 상태도 매출확정 제외 ← **가장 중요**
   - Line별 상태 확인
   - 배치 자동 재실행

6. **교환 회계처리**
   - 기존 취소 + 신규 생성 방식
   - 회계적 상쇄로 순효과 0 처리

7. **V2.5 CoA 규칙 적용**
   - 20개 계정 코드 체계
   - 6가지 거래유형별 전표 규칙
   - 부가세 동시 처리

8. **배송 예외 처리**
   - IN_TRANSIT_DELAYED/LOST/DAMAGED
   - DELIVERY_FAILED 분기

---

## 📊 상태 전이도

### 정상반품 프로세스
```
SO: DELIVERED
  ↓ (7일 이내 반품 요청)
Return: REQUESTED
  ↓ (반품팀 검토)
Return: UNDER_REVIEW
  ↓ (승인)
Return: APPROVED
  ↓ (환불 처리 + 전표 생성)
Return: REFUND_PROCESSED
  ↓
Return: CLOSED
```

### 하자반품 프로세스
```
SO: DELIVERED
  ↓ (7일 이내 반품 요청)
Return: REQUESTED
  ↓ (품질팀 검토)
Return: UNDER_REVIEW
  ↓ (승인)
Return: APPROVED
  ↓ (QC 검수)
QC_Result: DEFECT_CONFIRMED / NO_DEFECT_FOUND
  ├─ [폐기] → 손실 처리 → CLOSED
  ├─ [재입고] → 평가절하 처리 → CLOSED
  └─ [불량 아님] → 반품 거부 → CLOSED
```

### 교환 프로세스
```
SO(기존): DELIVERED
  ↓
Return: REQUESTED (type=EXCHANGE)
  ↓
Return: APPROVED
  ├─ [기존 주문 취소]
  ├─ [신규 주문 생성]
  └─
SO(신규): NEW
```

### 매출확정 배치
```
SO: DELIVERED (배송완료+10일)
  ↓ (배치 실행)
[조건 확인]
  ├─ 반품 미존재 → REVENUE_CONFIRMED
  ├─ 반품 진행 중 (REQUESTED/UNDER_REVIEW/APPROVED) → REVENUE_PENDING
  └─ 완료된 반품 회계처리 미완료 → REVENUE_PENDING
      ↓ (모든 반품 완료 후 재배치)
      → REVENUE_CONFIRMED
```

---

## ✅ 구현 체크리스트

- [ ] SO_Header.status 필드에 V2.4 상태값 반영
- [ ] Return_Header.status 필드에 V2.4 상태값 반영
- [ ] Return_Header.liability 필드 추가
- [ ] Transfer_Order.status 필드에 V2.4 상태값 반영
- [ ] Return_Line.restock_yn, damage_ratio 필드 추가
- [ ] QC_Result.damage_ratio 필드 추가
- [ ] 정상반품 검증 로직 (10일 기한)
- [ ] 하자반품 검증 로직 (7일 기한)
- [ ] 평가절하 로직 (50% 기본값)
- [ ] 환불 트랜잭션 생성 로직
- [ ] V2.5 CoA 계정 코드 설정 (20개)
- [ ] 거래유형별 전표 생성 로직
- [ ] 매출확정 배치 프로세스 (매일 자정)
- [ ] 배치 재실행 로직
- [ ] REVENUE 엔티티 관리
- [ ] 부분배송/부분반품 Line별 상태 관리
- [ ] 부가세 환입 동시 처리
- [ ] 배송 예외 처리 (FAILED/DELAYED/LOST)
- [ ] 교환 기존+신규 주문 동시 처리
- [ ] 로깅 및 에러 핸들링

---

*최종 작성일: 2025-11-06*  
*버전: V3.0*  
*기반: 상태머신 V2.4 + 매출확정 프로세스 V2.5 + CoA V2.5*
