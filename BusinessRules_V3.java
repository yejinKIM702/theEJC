/**
 * ========================================
 * 업데이트된 비즈니스 규칙 V2.5 (정상반품 3일)
 * (상태머신 V2.4 및 매출확정 프로세스 V2.5 기준)
 * ========================================
 * 
 * 주요 변경사항:
 * 1. 정상반품 기한: 배송 완료 후 10일 → 3일로 변경! (달력일 기준)
 * 2. 하자반품 기한: 배송 완료 후 7일 (유지)
 * 3. 매출확정 기한: 배송 완료 후 10일 (유지)
 * 4. 나머지 모든 규칙 유지 (평가절하, 책임소재 등)
 */

package com.erp.business.rules;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.math.BigDecimal;

public class ReturnAndRevenueBusinessRules_V25Modified {

    // ========================================
    // 상수 정의 (정상반품 3일로 변경!)
    // ========================================
    
    private static final int NORMAL_RETURN_DEADLINE_DAYS = 3;    // 변경됨: 10 → 3
    private static final int DEFECT_RETURN_DEADLINE_DAYS = 7;    // 유지
    private static final int REVENUE_CONFIRMATION_DAYS = 10;     // 유지

    /**
     * ==========================================
     * 5.1 정상반품 검증 규칙 (변경됨: 10일 → 3일)
     * ==========================================
     * V2.5-Modified 업데이트: 배송 완료 후 3일 이내만 정상반품 가능
     * 이유: 더 빠른 반품 처리로 운영 효율성 극대화
     */
    public static class NormalReturnValidation {
        
        /**
         * 정상반품 가능 여부 판정 (3일 기한)
         * @param so SO_Header
         * @param deliveredDt 배송완료일시
         * @param requestDt 반품요청일시
         * @param lineNo Line Item 번호
         * @param returnQty 반품수량
         * @return 가능/불가 여부 및 사유
         */
        public static ReturnValidationResult validateNormalReturn(
                SO_Header so,
                LocalDateTime deliveredDt,
                LocalDateTime requestDt,
                Integer lineNo,
                Integer returnQty) {
            
            ReturnValidationResult result = new ReturnValidationResult();
            
            // 조건 1: 배송완료 이전 반품 불가
            if (so.getStatus() == null || !isShipped(so.getStatus())) {
                result.setValid(false);
                result.setReason("반품은 배송 완료 이후에만 가능합니다");
                return result;
            }
            
            // 조건 2: 배송완료 후 3일 이내만 정상반품 가능 (변경됨!)
            // 달력일 기준 계산
            long daysBetween = ChronoUnit.DAYS.between(
                deliveredDt.toLocalDate(),
                requestDt.toLocalDate()
            );
            
            if (daysBetween > NORMAL_RETURN_DEADLINE_DAYS) {
                result.setValid(false);
                result.setReason(String.format(
                    "배송완료 후 %d일 이내만 정상반품 가능 (경과일: %d일)",
                    NORMAL_RETURN_DEADLINE_DAYS,
                    daysBetween
                ));
                return result;
            }
            
            // 조건 3: Line Item 반품수량 검증
            SO_Line soLine = so.getLineByNumber(lineNo);
            if (returnQty > soLine.getQty()) {
                result.setValid(false);
                result.setReason(String.format(
                    "반품수량(%d)이 주문수량(%d)을 초과할 수 없습니다",
                    returnQty,
                    soLine.getQty()
                ));
                return result;
            }
            
            // 조건 4: 부분반품 누적 검증
            Integer existingReturnQty = getExistingReturnQty(so.getSoNo(), lineNo, "NORMAL");
            if ((existingReturnQty + returnQty) > soLine.getQty()) {
                result.setValid(false);
                result.setReason(String.format(
                    "누적 반품수량(%d)이 주문수량(%d)을 초과합니다",
                    existingReturnQty + returnQty,
                    soLine.getQty()
                ));
                return result;
            }
            
            result.setValid(true);
            result.setReason("정상반품 조건 충족");
            return result;
        }
        
        private static boolean isShipped(String status) {
            return status.equals("WAYBILL_GENERATED") ||
                   status.equals("PICKUP_SCHEDULED") ||
                   status.equals("PICKED_UP") ||
                   status.equals("IN_TRANSIT") ||
                   status.equals("DELIVERED") ||
                   status.equals("PARTIALLY_DELIVERED") ||
                   status.equals("DELIVERY_FAILED") ||
                   status.equals("COMPLETED");
        }
    }

    /**
     * ==========================================
     * 5.2 하자반품 검증 규칙 (변경 없음: 7일 유지)
     * ==========================================
     */
    public static class DefectReturnValidation {
        
        /**
         * 하자반품 가능 여부 판정 (7일 기한 유지)
         */
        public static ReturnValidationResult validateDefectReturn(
                SO_Header so,
                LocalDateTime deliveredDt,
                LocalDateTime requestDt,
                Integer lineNo,
                Integer returnQty) {
            
            ReturnValidationResult result = new ReturnValidationResult();
            
            // 조건 1: 배송완료 이후만 하자반품 가능
            if (so.getStatus() == null || !isDelivered(so.getStatus())) {
                result.setValid(false);
                result.setReason("하자반품은 배송 완료 이후만 가능합니다");
                return result;
            }
            
            // 조건 2: 배송완료 후 7일 이내 (AS 기한, 변경 없음)
            long daysBetween = ChronoUnit.DAYS.between(
                deliveredDt.toLocalDate(),
                requestDt.toLocalDate()
            );
            
            if (daysBetween > DEFECT_RETURN_DEADLINE_DAYS) {
                result.setValid(false);
                result.setReason(String.format(
                    "AS 기한은 배송완료 후 %d일입니다 (경과일: %d일)",
                    DEFECT_RETURN_DEADLINE_DAYS,
                    daysBetween
                ));
                return result;
            }
            
            // 조건 3: 반품수량 검증
            SO_Line soLine = so.getLineByNumber(lineNo);
            if (returnQty > soLine.getQty()) {
                result.setValid(false);
                result.setReason(String.format(
                    "반품수량(%d)이 주문수량(%d)을 초과할 수 없습니다",
                    returnQty,
                    soLine.getQty()
                ));
                return result;
            }
            
            // 조건 4: 부분반품 누적 검증
            Integer existingReturnQty = getExistingReturnQty(so.getSoNo(), lineNo, "DEFECT");
            if ((existingReturnQty + returnQty) > soLine.getQty()) {
                result.setValid(false);
                result.setReason(String.format(
                    "누적 반품수량(%d)이 주문수량(%d)을 초과합니다",
                    existingReturnQty + returnQty,
                    soLine.getQty()
                ));
                return result;
            }
            
            result.setValid(true);
            result.setReason("하자반품 조건 충족");
            return result;
        }
        
        private static boolean isDelivered(String status) {
            return status.equals("DELIVERED") ||
                   status.equals("PARTIALLY_DELIVERED") ||
                   status.equals("COMPLETED");
        }
    }

    /**
     * ==========================================
     * 5.3 반품 환불 처리 및 회계 전표 생성 (변경 없음)
     * ==========================================
     */
    public static class RefundAndJournalProcessing {
        
        /**
         * 정상반품 환불 처리 (3일 기한 동안)
         */
        public static List<AccountingJournal> processNormalReturnRefund(
                Return_Header returnHeader,
                List<Return_Line> returnLines) {
            
            List<AccountingJournal> journals = new ArrayList<>();
            BigDecimal totalRefundAmount = BigDecimal.ZERO;
            BigDecimal totalTaxAmount = BigDecimal.ZERO;
            BigDecimal totalInventoryCost = BigDecimal.ZERO;
            
            String soNo = returnHeader.getSoNo();
            SO_Header so = getSO(soNo);
            LocalDateTime refundDt = LocalDateTime.now();
            
            // 반품수량 및 환불액 계산
            for (Return_Line line : returnLines) {
                SO_Line soLine = so.getLineByNumber(line.getLineNo());
                BigDecimal lineRefundAmount = soLine.getUnitPrice()
                    .multiply(BigDecimal.valueOf(line.getReturnQty()));
                BigDecimal lineTaxAmount = lineRefundAmount
                    .multiply(BigDecimal.valueOf(soLine.getTaxRate()));
                
                totalRefundAmount = totalRefundAmount.add(lineRefundAmount);
                totalTaxAmount = totalTaxAmount.add(lineTaxAmount);
                totalInventoryCost = totalInventoryCost.add(
                    getInventoryCost(line.getItemId())
                        .multiply(BigDecimal.valueOf(line.getReturnQty()))
                );
            }
            
            // 전표 생성 (V2.5 CoA 규칙)
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-1",
                refundDt,
                "4020",  // 매출환입
                "1050",  // 미지급금(고객환불)
                totalRefundAmount,
                "정상반품 매출환입 (3일 이내): " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-2",
                refundDt,
                "1060",  // 부가세예수금
                "2040",  // 부가세납부금
                totalTaxAmount,
                "정상반품 부가세환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-3",
                refundDt,
                "1020",  // 상품
                "5010",  // 매출원가
                totalInventoryCost,
                "정상반품 재고복구: " + returnHeader.getReturnNo()
            ));
            
            // 환불 트랜잭션 생성
            Refund_Transaction refund = new Refund_Transaction(
                "REFUND-" + returnHeader.getReturnNo(),
                returnHeader.getReturnNo(),
                refundDt,
                so.getPaymentMethod(),
                totalRefundAmount.add(totalTaxAmount),
                "PENDING",
                "정상반품 환불 (3일 기한)"
            );
            refund.save();
            
            return journals;
        }
        
        // 하자반품 (폐기) - 변경 없음
        public static List<AccountingJournal> processDefectReturnRefundDisposal(
                Return_Header returnHeader,
                List<Return_Line> returnLines,
                List<QC_Result> qcResults) {
            
            List<AccountingJournal> journals = new ArrayList<>();
            BigDecimal totalRefundAmount = BigDecimal.ZERO;
            BigDecimal totalTaxAmount = BigDecimal.ZERO;
            BigDecimal totalLossAmount = BigDecimal.ZERO;
            
            String soNo = returnHeader.getSoNo();
            SO_Header so = getSO(soNo);
            LocalDateTime refundDt = LocalDateTime.now();
            
            for (Return_Line line : returnLines) {
                QC_Result qcResult = qcResults.stream()
                    .filter(q -> q.getItemId().equals(line.getItemId()))
                    .findFirst()
                    .orElse(null);
                
                if (qcResult == null || !qcResult.getResult().equals("DEFECT_CONFIRMED")) {
                    continue;
                }
                
                SO_Line soLine = so.getLineByNumber(line.getLineNo());
                BigDecimal lineRefundAmount = soLine.getUnitPrice()
                    .multiply(BigDecimal.valueOf(line.getReturnQty()));
                BigDecimal lineTaxAmount = lineRefundAmount
                    .multiply(BigDecimal.valueOf(soLine.getTaxRate()));
                BigDecimal lineLossAmount = getInventoryCost(line.getItemId())
                    .multiply(BigDecimal.valueOf(line.getReturnQty()));
                
                totalRefundAmount = totalRefundAmount.add(lineRefundAmount);
                totalTaxAmount = totalTaxAmount.add(lineTaxAmount);
                totalLossAmount = totalLossAmount.add(lineLossAmount);
            }
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-1",
                refundDt,
                "4020",  // 매출환입
                "1050",  // 미지급금(고객환불)
                totalRefundAmount,
                "하자반품(폐기) 매출환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-2",
                refundDt,
                "1060",  // 부가세예수금
                "2040",  // 부가세납부금
                totalTaxAmount,
                "하자반품 부가세환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-3",
                refundDt,
                "6010",  // 재고평가손실
                "5010",  // 매출원가
                totalLossAmount,
                "하자반품(폐기) 손실 인식: " + returnHeader.getReturnNo()
            ));
            
            Refund_Transaction refund = new Refund_Transaction(
                "REFUND-" + returnHeader.getReturnNo(),
                returnHeader.getReturnNo(),
                refundDt,
                so.getPaymentMethod(),
                totalRefundAmount.add(totalTaxAmount),
                "PENDING",
                "하자반품(폐기) 환불"
            );
            refund.save();
            
            return journals;
        }
        
        // 하자반품 (재입고, 평가절하) - 변경 없음
        public static List<AccountingJournal> processDefectReturnRestockWithValuation(
                Return_Header returnHeader,
                List<Return_Line> returnLines,
                Double damageRatio) {
            
            List<AccountingJournal> journals = new ArrayList<>();
            BigDecimal totalRefundAmount = BigDecimal.ZERO;
            BigDecimal totalTaxAmount = BigDecimal.ZERO;
            BigDecimal totalRestockCost = BigDecimal.ZERO;
            BigDecimal totalLossAmount = BigDecimal.ZERO;
            
            String soNo = returnHeader.getSoNo();
            SO_Header so = getSO(soNo);
            LocalDateTime restockDt = LocalDateTime.now();
            
            if (damageRatio == null) {
                damageRatio = 0.5;  // 기본 50% 평가절하
            }
            
            for (Return_Line line : returnLines) {
                if (!line.isRestockYn()) {
                    continue;
                }
                
                SO_Line soLine = so.getLineByNumber(line.getLineNo());
                BigDecimal lineRefundAmount = soLine.getUnitPrice()
                    .multiply(BigDecimal.valueOf(line.getReturnQty()));
                BigDecimal lineTaxAmount = lineRefundAmount
                    .multiply(BigDecimal.valueOf(soLine.getTaxRate()));
                
                BigDecimal inventoryCost = getInventoryCost(line.getItemId())
                    .multiply(BigDecimal.valueOf(line.getReturnQty()));
                BigDecimal restockCost = inventoryCost
                    .multiply(BigDecimal.valueOf(1.0 - damageRatio));
                BigDecimal lossAmount = inventoryCost.subtract(restockCost);
                
                totalRefundAmount = totalRefundAmount.add(lineRefundAmount);
                totalTaxAmount = totalTaxAmount.add(lineTaxAmount);
                totalRestockCost = totalRestockCost.add(restockCost);
                totalLossAmount = totalLossAmount.add(lossAmount);
            }
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-1",
                restockDt,
                "4020",  // 매출환입
                "1050",  // 미지급금(고객환불)
                totalRefundAmount,
                "하자반품(재입고) 매출환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-2",
                restockDt,
                "1060",  // 부가세예수금
                "2040",  // 부가세납부금
                totalTaxAmount,
                "하자반품 부가세환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-3",
                restockDt,
                "1030",  // 상품(평가절하)
                "5010",  // 매출원가
                totalRestockCost,
                "하자반품(재입고) 평가절하 재고: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-4",
                restockDt,
                "6010",  // 재고평가손실
                "5010",  // 매출원가
                totalLossAmount,
                "하자반품(재입고) 손실: " + returnHeader.getReturnNo()
            ));
            
            Refund_Transaction refund = new Refund_Transaction(
                "REFUND-" + returnHeader.getReturnNo(),
                returnHeader.getReturnNo(),
                restockDt,
                so.getPaymentMethod(),
                totalRefundAmount.add(totalTaxAmount),
                "PENDING",
                "하자반품(재입고) 환불"
            );
            refund.save();
            
            return journals;
        }
        
        // 교환 처리 - 변경 없음
        public static List<AccountingJournal> processExchange(
                Return_Header returnHeader,
                String newSoNo) {
            
            List<AccountingJournal> journals = new ArrayList<>();
            String soNo = returnHeader.getSoNo();
            SO_Header originalSo = getSO(soNo);
            SO_Header newSo = getSO(newSoNo);
            LocalDateTime exchangeDt = LocalDateTime.now();
            
            BigDecimal totalAmount = originalSo.getAmount();
            BigDecimal totalInventoryCost = calculateTotalInventoryCost(originalSo);
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-1",
                exchangeDt,
                "4020",  // 매출환입
                "1050",  // 미지급금(고객환불)
                totalAmount,
                "교환 기존주문 환입: " + returnHeader.getReturnNo()
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-2",
                exchangeDt,
                "1020",  // 상품
                "5010",  // 매출원가
                totalInventoryCost,
                "교환 기존상품 반품: " + returnHeader.getReturnNo()
            ));
            
            BigDecimal newTotalInventoryCost = calculateTotalInventoryCost(newSo);
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-3",
                exchangeDt,
                "1050",  // 미지급금
                "4010",  // 매출
                totalAmount,
                "교환 신규주문 매출: " + newSoNo
            ));
            
            journals.add(new AccountingJournal(
                "JE-" + returnHeader.getReturnNo() + "-4",
                exchangeDt,
                "5010",  // 매출원가
                "1020",  // 상품
                newTotalInventoryCost,
                "교환 신규상품 원가: " + newSoNo
            ));
            
            if (newSo.getAmount().compareTo(originalSo.getAmount()) != 0) {
                BigDecimal difAmount = newSo.getAmount().subtract(originalSo.getAmount());
                journals.add(new AccountingJournal(
                    "JE-" + returnHeader.getReturnNo() + "-5",
                    exchangeDt,
                    difAmount.compareTo(BigDecimal.ZERO) > 0 ? "1050" : "4010",
                    difAmount.compareTo(BigDecimal.ZERO) > 0 ? "4010" : "1050",
                    difAmount.abs(),
                    "교환 금액 차액 조정: " + returnHeader.getReturnNo()
                ));
            }
            
            return journals;
        }
    }

    /**
     * ==========================================
     * 5.4 매출확정 배치 (변경 없음: 10일 유지)
     * ==========================================
     * 배송 완료 후 10일 경과, 반품 상태 전체 확인
     * 정상반품은 3일 이내만 받으므로, 3일 이후 반품 요청은 거부됨
     */
    public static class RevenueConfirmationBatch {
        
        /**
         * 매출확정 배치 실행 (매일 자정)
         * @return 처리된 주문 건수
         */
        public static Integer executeRevenueConfirmationBatch() {
            Integer processedCount = 0;
            LocalDateTime batchExecutionTime = LocalDateTime.now();
            
            List<SO_Header> targetOrders = querySalesOrdersForRevenueConfirmation(
                batchExecutionTime
            );
            
            Logger logger = new Logger("RevenueConfirmationBatch");
            logger.info(String.format(
                "[배치 시작] 실행시간: %s, 대상건수: %d",
                batchExecutionTime,
                targetOrders.size()
            ));
            
            for (SO_Header so : targetOrders) {
                try {
                    RevenueConfirmationResult result = 
                        judgeRevenueConfirmation(so, batchExecutionTime);
                    
                    if (result.isConfirmable()) {
                        confirmRevenue(so, result.getJournals());
                        processedCount++;
                        
                        logger.info(String.format(
                            "[매출확정 완료] SO: %s, 금액: %d원 (배송 후 %d일)",
                            so.getSoNo(),
                            so.getAmount(),
                            REVENUE_CONFIRMATION_DAYS
                        ));
                        
                    } else {
                        updateSOStatus(so.getSoNo(), "REVENUE_PENDING");
                        
                        logger.warn(String.format(
                            "[반품 대기] SO: %s, 사유: %s",
                            so.getSoNo(),
                            result.getReason()
                        ));
                    }
                    
                } catch (Exception e) {
                    logger.error(String.format(
                        "[오류] SO: %s, 메시지: %s",
                        so.getSoNo(),
                        e.getMessage()
                    ));
                }
            }
            
            logger.info(String.format(
                "[배치 완료] 처리건수: %d/%d",
                processedCount,
                targetOrders.size()
            ));
            
            return processedCount;
        }
        
        /**
         * 매출확정 대상 주문 조회 (10일 경과 기준)
         */
        private static List<SO_Header> querySalesOrdersForRevenueConfirmation(
                LocalDateTime executionTime) {
            
            String sql = """
                SELECT * FROM so_header
                WHERE status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
                  AND revenue_confirmed_dt IS NULL
                  AND DATE_ADD(delivered_dt, INTERVAL 10 DAY) <= CURDATE()
                  AND NOT EXISTS (
                    SELECT 1 FROM return_header
                    WHERE return_header.so_no = so_header.so_no
                      AND return_header.status IN ('REQUESTED', 'UNDER_REVIEW', 'APPROVED')
                  )
                ORDER BY delivered_dt ASC
            """;
            
            return executeQuery(sql);
        }
        
        /**
         * 매출확정 판정 로직
         */
        private static RevenueConfirmationResult judgeRevenueConfirmation(
                SO_Header so,
                LocalDateTime batchTime) {
            
            RevenueConfirmationResult result = new RevenueConfirmationResult();
            List<AccountingJournal> journals = new ArrayList<>();
            
            // 조건 1: 배송 완료 후 10일 경과 (변경 없음)
            long daysSinceDelivery = ChronoUnit.DAYS.between(
                so.getDeliveredDt().toLocalDate(),
                batchTime.toLocalDate()
            );
            
            if (daysSinceDelivery < REVENUE_CONFIRMATION_DAYS) {
                result.setConfirmable(false);
                result.setReason(String.format(
                    "배송완료 후 %d일 경과 (10일 필요)",
                    daysSinceDelivery
                ));
                return result;
            }
            
            // 조건 2: 반품 상태 전체 확인 (REQUESTED 포함)
            List<Return_Header> activeReturns = getActiveReturns(so.getSoNo());
            
            if (!activeReturns.isEmpty()) {
                String returnStatuses = activeReturns.stream()
                    .map(Return_Header::getStatus)
                    .collect(Collectors.joining(", "));
                
                result.setConfirmable(false);
                result.setReason(String.format(
                    "활성 반품 존재 [상태: %s]",
                    returnStatuses
                ));
                return result;
            }
            
            // 조건 3: 완료된 반품의 회계처리 확인
            List<Return_Header> completedReturns = getCompletedReturns(so.getSoNo());
            
            if (!completedReturns.isEmpty()) {
                for (Return_Header returnHeader : completedReturns) {
                    if (!isReturnAccountingCompleted(returnHeader)) {
                        result.setConfirmable(false);
                        result.setReason(String.format(
                            "반품 %s의 회계처리 미완료",
                            returnHeader.getReturnNo()
                        ));
                        return result;
                    }
                }
            }
            
            // 조건 4: 부분배송/부분반품 시 Line별 상태 확인
            boolean isPartialDelivery = so.getStatus().equals("PARTIALLY_DELIVERED");
            boolean isPartialReturn = hasPartialReturn(so.getSoNo());
            
            if (isPartialDelivery || isPartialReturn) {
                List<SO_Line> soLines = so.getLines();
                for (SO_Line line : soLines) {
                    Integer lineReturnQty = getLineReturnQty(so.getSoNo(), line.getLineNo());
                    Integer lineDeliveredQty = getLineDeliveredQty(so.getSoNo(), line.getLineNo());
                    
                    Integer confirmedQty = lineDeliveredQty - lineReturnQty;
                    if (confirmedQty <= 0) {
                        continue;
                    }
                }
            }
            
            // 매출확정 처리
            journals.add(new AccountingJournal(
                "JE-REVENUE-" + so.getSoNo(),
                batchTime,
                "5010",  // 매출원가
                "1020",  // 상품
                calculateTotalInventoryCost(so),
                "매출확정(배송완료+10일): " + so.getSoNo()
            ));
            
            result.setConfirmable(true);
            result.setReason(String.format(
                "매출확정 조건 충족 (배송 후 %d일 경과, 정상반품 기한 3일 이내)",
                daysSinceDelivery
            ));
            result.setJournals(journals);
            result.setConfirmationTime(batchTime);
            
            return result;
        }
        
        private static void confirmRevenue(
                SO_Header so,
                List<AccountingJournal> journals) {
            
            for (AccountingJournal journal : journals) {
                journal.save();
            }
            
            so.setStatus("COMPLETED");
            so.setRevenueConfirmedDt(LocalDateTime.now());
            so.save();
            
            Revenue revenue = new Revenue(
                "REV-" + so.getSoNo(),
                so.getSoNo(),
                "REVENUE_CONFIRMED",
                so.getAmount(),
                LocalDateTime.now(),
                "배송완료+10일 자동 매출확정 (정상반품 3일 기한)"
            );
            revenue.save();
        }
        
        private static List<Return_Header> getActiveReturns(String soNo) {
            String sql = """
                SELECT * FROM return_header
                WHERE so_no = ?
                  AND status IN ('REQUESTED', 'UNDER_REVIEW', 'APPROVED')
            """;
            return executeQuery(sql, soNo);
        }
        
        private static List<Return_Header> getCompletedReturns(String soNo) {
            String sql = """
                SELECT * FROM return_header
                WHERE so_no = ?
                  AND status IN ('CLOSED', 'COMPLETED')
            """;
            return executeQuery(sql, soNo);
        }
    }

    // ========================================
    // 헬퍼 메소드 (변경 사항 적용)
    // ========================================
    
    private static Integer getExistingReturnQty(String soNo, Integer lineNo, String returnType) {
        String sql = """
            SELECT COALESCE(SUM(return_qty), 0) as total_return_qty
            FROM return_line
            WHERE return_no IN (
              SELECT return_no FROM return_header
              WHERE so_no = ?
                AND type = ?
                AND status IN ('APPROVED', 'ITEM_RECEIVED', 'CLOSED')
            )
            AND line_no = ?
        """;
        return executeQueryScalar(sql, soNo, returnType, lineNo);
    }
    
    private static BigDecimal getInventoryCost(String itemId) {
        String sql = """
            SELECT COALESCE(avg_cost, 0) as cost
            FROM item_master
            WHERE item_id = ?
        """;
        return (BigDecimal) executeQueryScalar(sql, itemId);
    }
    
    private static BigDecimal calculateTotalInventoryCost(SO_Header so) {
        BigDecimal total = BigDecimal.ZERO;
        for (SO_Line line : so.getLines()) {
            total = total.add(
                getInventoryCost(line.getItemId())
                    .multiply(BigDecimal.valueOf(line.getQty()))
            );
        }
        return total;
    }
    
    private static boolean hasPartialReturn(String soNo) {
        String sql = """
            SELECT COUNT(*) as cnt
            FROM return_header
            WHERE so_no = ?
              AND type IN ('NORMAL', 'DEFECT')
              AND status = 'APPROVED'
        """;
        return ((Number) executeQueryScalar(sql, soNo)).intValue() > 0;
    }
    
    private static Integer getLineReturnQty(String soNo, Integer lineNo) {
        String sql = """
            SELECT COALESCE(SUM(return_qty), 0) as total
            FROM return_line
            WHERE line_no = ?
              AND return_no IN (
                SELECT return_no FROM return_header
                WHERE so_no = ? AND status = 'APPROVED'
              )
        """;
        return ((Number) executeQueryScalar(sql, lineNo, soNo)).intValue();
    }
    
    private static Integer getLineDeliveredQty(String soNo, Integer lineNo) {
        String sql = """
            SELECT COALESCE(SUM(qty), 0) as total
            FROM so_line
            WHERE so_no = ? AND line_no = ?
        """;
        return ((Number) executeQueryScalar(sql, soNo, lineNo)).intValue();
    }
    
    private static boolean isReturnAccountingCompleted(Return_Header returnHeader) {
        String sql = """
            SELECT COUNT(*) as cnt
            FROM accounting_journal
            WHERE return_no = ?
              AND dr_acct IS NOT NULL
              AND cr_acct IS NOT NULL
        """;
        return ((Number) executeQueryScalar(sql, returnHeader.getReturnNo())).intValue() > 0;
    }
    
    private static SO_Header getSO(String soNo) {
        return SO_Header.findByNo(soNo);
    }
    
    private static void updateSOStatus(String soNo, String newStatus) {
        SO_Header so = getSO(soNo);
        so.setStatus(newStatus);
        so.save();
    }
    
    private static List<Object> executeQuery(String sql, Object... params) {
        return null;
    }
    
    private static Object executeQueryScalar(String sql, Object... params) {
        return null;
    }
}

/**
 * ==========================================
 * 보조 클래스
 * ==========================================
 */

class ReturnValidationResult {
    private boolean valid;
    private String reason;
    
    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}

class RevenueConfirmationResult {
    private boolean confirmable;
    private String reason;
    private List<AccountingJournal> journals;
    private LocalDateTime confirmationTime;
    
    public boolean isConfirmable() { return confirmable; }
    public void setConfirmable(boolean confirmable) { this.confirmable = confirmable; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public List<AccountingJournal> getJournals() { return journals; }
    public void setJournals(List<AccountingJournal> journals) { this.journals = journals; }
    public LocalDateTime getConfirmationTime() { return confirmationTime; }
    public void setConfirmationTime(LocalDateTime confirmationTime) { this.confirmationTime = confirmationTime; }
}

class AccountingJournal {
    private String journalId;
    private LocalDateTime eventDt;
    private String drAcct;
    private String crAcct;
    private BigDecimal amount;
    private String memo;
    
    public AccountingJournal(String journalId, LocalDateTime eventDt,
                             String drAcct, String crAcct,
                             BigDecimal amount, String memo) {
        this.journalId = journalId;
        this.eventDt = eventDt;
        this.drAcct = drAcct;
        this.crAcct = crAcct;
        this.amount = amount;
        this.memo = memo;
    }
    
    public void save() {}
}

class Refund_Transaction {
    private String refundId;
    private String returnNo;
    private LocalDateTime refundDt;
    private String refundMethod;
    private BigDecimal amount;
    private String status;
    private String memo;
    
    public Refund_Transaction(String refundId, String returnNo,
                              LocalDateTime refundDt, String refundMethod,
                              BigDecimal amount, String status, String memo) {
        this.refundId = refundId;
        this.returnNo = returnNo;
        this.refundDt = refundDt;
        this.refundMethod = refundMethod;
        this.amount = amount;
        this.status = status;
        this.memo = memo;
    }
    
    public void save() {}
}

class Revenue {
    private String revenueId;
    private String soNo;
    private String status;
    private BigDecimal amount;
    private LocalDateTime confirmedDt;
    private String memo;
    
    public Revenue(String revenueId, String soNo, String status,
                   BigDecimal amount, LocalDateTime confirmedDt, String memo) {
        this.revenueId = revenueId;
        this.soNo = soNo;
        this.status = status;
        this.amount = amount;
        this.confirmedDt = confirmedDt;
        this.memo = memo;
    }
    
    public void save() {}
}

class Logger {
    private String name;
    
    public Logger(String name) {
        this.name = name;
    }
    
    public void info(String message) {
        System.out.println("[INFO] " + name + ": " + message);
    }
    
    public void warn(String message) {
        System.out.println("[WARN] " + name + ": " + message);
    }
    
    public void error(String message) {
        System.err.println("[ERROR] " + name + ": " + message);
    }
}

// ========================================
// END OF V2.5-MODIFIED JAVA (정상반품 3일)
// ========================================
