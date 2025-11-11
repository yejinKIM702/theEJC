/**
 * ========================================
 * SCM 시스템 V2.5 MySQL 쿼리
 * (정상반품 기한: 3일로 변경)
 * ========================================
 * 
 * 주요 변경사항 (V2.5 → V2.5-Modified):
 * 1. 정상반품 기한: 배송 완료 후 10일 → 3일 (달력일 기준)
 * 2. 하자반품 기한: 유지 (배송 완료 후 7일)
 * 3. 매출확정 배치: 반품 대기 기간 3일 → 10일 유지 (배송 완료 후)
 * 4. REQUESTED 상태 포함 반품 확인 (기존과 동일)
 * 5. 나머지 모든 요소 유지 (CoA, 상태머신, 회계 규칙)
 */

-- =====================================================
-- 1. 기본 마스터 테이블 (변경 없음)
-- =====================================================

CREATE TABLE IF NOT EXISTS customer (
  customer_id      VARCHAR(40) PRIMARY KEY,
  name             VARCHAR(100) NOT NULL,
  email            VARCHAR(100),
  phone            VARCHAR(30),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS item_master (
  item_id          VARCHAR(40) PRIMARY KEY,
  name             VARCHAR(100) NOT NULL,
  spec             TEXT,
  kpc_cert_no      VARCHAR(40),
  origin           VARCHAR(50),
  avg_cost         DECIMAL(18,4),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 2. 포장재 및 라이선스 (변경 없음)
-- =====================================================

CREATE TABLE IF NOT EXISTS packaging_material_master (
  pm_id            INT AUTO_INCREMENT PRIMARY KEY,
  name             VARCHAR(100) NOT NULL,
  type             VARCHAR(30),
  spec             TEXT,
  uom              VARCHAR(10),
  supplier_id      VARCHAR(40),
  safety_stock     DECIMAL(18,3),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS license_contract (
  license_id       INT AUTO_INCREMENT PRIMARY KEY,
  licensor         VARCHAR(100),
  contract_no      VARCHAR(40) UNIQUE,
  status           VARCHAR(20),
  start_dt         DATE,
  end_dt           DATE,
  royalty_rate     DECIMAL(5,2),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 3. PO (발주) - V2.4 상태머신 반영
-- =====================================================

CREATE TABLE IF NOT EXISTS po_header (
  po_no            VARCHAR(40) PRIMARY KEY,
  vendor_id        VARCHAR(40) NOT NULL,
  status           VARCHAR(30) NOT NULL,
  order_dt         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  eta_dt           TIMESTAMP,
  approved_dt      TIMESTAMP,
  in_production_dt TIMESTAMP,
  shipped_dt       TIMESTAMP,
  customs_clear_dt TIMESTAMP,
  customs_failed_dt TIMESTAMP,
  customs_failed_reason VARCHAR(200),
  three_pl_inbound_dt TIMESTAMP,
  three_pl_rejected_dt TIMESTAMP,
  three_pl_rejected_reason VARCHAR(200),
  qc_passed_dt     TIMESTAMP,
  qc_failed_dt     TIMESTAMP,
  qc_failed_reason VARCHAR(200),
  transfer_issued_dt TIMESTAMP,
  closed_dt        TIMESTAMP,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_status(status),
  INDEX idx_vendor_id(vendor_id)
);

CREATE TABLE IF NOT EXISTS po_line (
  po_no            VARCHAR(40) NOT NULL,
  line_no          INT NOT NULL,
  item_id          VARCHAR(40) NOT NULL,
  qty              DECIMAL(18,3) NOT NULL,
  price            DECIMAL(18,4),
  currency         VARCHAR(10) DEFAULT 'KRW',
  qty_received     DECIMAL(18,3) DEFAULT 0,
  partial_received_dt TIMESTAMP,
  PRIMARY KEY (po_no, line_no),
  FOREIGN KEY (po_no) REFERENCES po_header(po_no) ON DELETE CASCADE,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id)
);

-- =====================================================
-- 4. SO (주문) - V2.4 상태머신 + 정상반품 3일 반영
-- =====================================================

CREATE TABLE IF NOT EXISTS so_header (
  so_no            VARCHAR(40) PRIMARY KEY,
  customer_id      VARCHAR(40) NOT NULL,
  status           VARCHAR(30) NOT NULL,
  order_dt         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  pay_status       VARCHAR(20),
  delivered_dt     TIMESTAMP,
  -- V2.5 매출확정 필드 (정상반품 3일 반영)
  normal_return_due_dt TIMESTAMP,    -- 배송완료+3일 (정상반품 기한)
  return_deferral_due_dt TIMESTAMP,  -- 배송완료+10일 (반품 대기, 매출확정 기한)
  revenue_confirmed_dt TIMESTAMP,
  amount           DECIMAL(18,2) NOT NULL,
  tax              DECIMAL(18,2),
  channel          VARCHAR(30),
  pack_ready_dt    TIMESTAMP,
  packing_in_progress_dt TIMESTAMP,
  packed_dt        TIMESTAMP,
  picking_done_dt  TIMESTAMP,
  waybill_generated_dt TIMESTAMP,
  waybill_no       VARCHAR(40),
  pickup_scheduled_dt TIMESTAMP,
  picked_up_dt     TIMESTAMP,
  in_transit_dt    TIMESTAMP,
  delivery_failed_dt TIMESTAMP,
  partially_delivered_dt TIMESTAMP,
  completed_dt     TIMESTAMP,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_status(status),
  INDEX idx_customer_id(customer_id),
  INDEX idx_delivered_dt(delivered_dt),
  INDEX idx_normal_return_due_dt(normal_return_due_dt),
  INDEX idx_return_deferral_due_dt(return_deferral_due_dt)
);

CREATE TABLE IF NOT EXISTS so_line (
  so_no            VARCHAR(40) NOT NULL,
  line_no          INT NOT NULL,
  item_id          VARCHAR(40) NOT NULL,
  qty              DECIMAL(18,3) NOT NULL,
  unit_price       DECIMAL(18,4),
  tax_rate         DECIMAL(5,4),
  qty_delivered    DECIMAL(18,3) DEFAULT 0,
  qty_returned     DECIMAL(18,3) DEFAULT 0,
  qty_confirmed    DECIMAL(18,3) DEFAULT 0,
  PRIMARY KEY (so_no, line_no),
  FOREIGN KEY (so_no) REFERENCES so_header(so_no) ON DELETE CASCADE,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id)
);

-- =====================================================
-- 5. SO_Charges
-- =====================================================

CREATE TABLE IF NOT EXISTS so_charges (
  so_charges_id    INT AUTO_INCREMENT PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  charge_type      VARCHAR(40) NOT NULL,
  amount           DECIMAL(18,2) NOT NULL,
  currency         VARCHAR(10) DEFAULT 'KRW',
  description      TEXT,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (so_no) REFERENCES so_header(so_no) ON DELETE CASCADE,
  INDEX idx_so_no(so_no)
);

-- =====================================================
-- 6. Delivery_Address
-- =====================================================

CREATE TABLE IF NOT EXISTS delivery_address (
  delivery_id      INT AUTO_INCREMENT PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  recipient_name   VARCHAR(60) NOT NULL,
  phone            VARCHAR(30),
  address          TEXT NOT NULL,
  postal_code      VARCHAR(20),
  special_instruction TEXT,
  confirmed_dt     TIMESTAMP,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (so_no) REFERENCES so_header(so_no) ON DELETE CASCADE,
  INDEX idx_so_no(so_no)
);

-- =====================================================
-- 7. Shipment
-- =====================================================

CREATE TABLE IF NOT EXISTS shipment (
  waybill_no       VARCHAR(40) PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  carrier          VARCHAR(50),
  pickup_dt        TIMESTAMP,
  in_transit_dt    TIMESTAMP,
  delivered_dt     TIMESTAMP,
  delivery_failed_dt TIMESTAMP,
  in_transit_delayed_dt TIMESTAMP,
  in_transit_lost_dt TIMESTAMP,
  damaged_dt       TIMESTAMP,
  FOREIGN KEY (so_no) REFERENCES so_header(so_no),
  INDEX idx_so_no(so_no)
);

-- =====================================================
-- 8. TRANSFER (3PL→당사 이동) - V2.4 상태머신
-- =====================================================

CREATE TABLE IF NOT EXISTS transfer_order (
  transfer_no      VARCHAR(40) PRIMARY KEY,
  po_no            VARCHAR(40) NOT NULL,
  status           VARCHAR(30) NOT NULL,
  request_dt       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  transfer_req_approved_dt TIMESTAMP,
  three_pl_ready_dt TIMESTAMP,
  three_pl_dispatched_dt TIMESTAMP,
  in_transit_dt    TIMESTAMP,
  in_transit_delayed_dt TIMESTAMP,
  in_transit_lost_dt TIMESTAMP,
  damaged_dt       TIMESTAMP,
  wms_received_dt  TIMESTAMP,
  lot_assigned_dt  TIMESTAMP,
  qc_inbound_in_progress_dt TIMESTAMP,
  qc_passed_dt     TIMESTAMP,
  qc_failed_dt     TIMESTAMP,
  stock_updated_dt TIMESTAMP,
  return_to_3pl_dt TIMESTAMP,
  closed_dt        TIMESTAMP,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (po_no) REFERENCES po_header(po_no),
  INDEX idx_status(status),
  INDEX idx_po_no(po_no)
);

-- =====================================================
-- 9. Transfer_In (이동입고 상세) - V2.5 평가절하 반영
-- =====================================================

CREATE TABLE IF NOT EXISTS transfer_in (
  transfer_in_id   INT AUTO_INCREMENT PRIMARY KEY,
  transfer_no      VARCHAR(40) NOT NULL,
  item_id          VARCHAR(40),
  received_dt      TIMESTAMP,
  qc_status        VARCHAR(20),
  lot_no           VARCHAR(40),
  location         VARCHAR(100),
  qty_received     DECIMAL(18,3),
  qty_defect       DECIMAL(18,3),
  damage_ratio     DECIMAL(5,4) DEFAULT 0.0,
  restock_yn       BOOLEAN DEFAULT FALSE,
  damage_qty       DECIMAL(18,3) DEFAULT 0,
  restock_qty      DECIMAL(18,3) DEFAULT 0,
  valuation_cost   DECIMAL(18,4),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (transfer_no) REFERENCES transfer_order(transfer_no) ON DELETE CASCADE,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id),
  INDEX idx_transfer_no(transfer_no),
  INDEX idx_lot_no(lot_no)
);

-- =====================================================
-- 10. RETURN (반품) - V2.4/V2.5 + 정상반품 3일 변경
-- =====================================================

CREATE TABLE IF NOT EXISTS return_header (
  return_no        VARCHAR(40) PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  type             VARCHAR(15) NOT NULL,  -- NORMAL (3일), DEFECT (7일), EXCHANGE
  status           VARCHAR(30) NOT NULL,
  reason           TEXT,
  request_dt       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  under_review_dt  TIMESTAMP,
  approved_dt      TIMESTAMP,
  approved_by      VARCHAR(40),
  rejected_dt      TIMESTAMP,
  rejection_reason VARCHAR(200),
  item_return_initiated_dt TIMESTAMP,
  item_received_dt TIMESTAMP,
  qc_return_in_progress_dt TIMESTAMP,
  qc_return_passed_dt TIMESTAMP,
  qc_result        VARCHAR(20),
  qc_by            VARCHAR(40),
  refund_processed_dt TIMESTAMP,
  closed_dt        TIMESTAMP,
  liability        VARCHAR(30),
  liability_determined_dt TIMESTAMP,
  damage_ratio     DECIMAL(5,4) DEFAULT 0.5,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (so_no) REFERENCES so_header(so_no),
  INDEX idx_status(status),
  INDEX idx_type(type),
  INDEX idx_so_no(so_no),
  INDEX idx_request_dt(request_dt)
);

-- =====================================================
-- 11. Return_Line
-- =====================================================

CREATE TABLE IF NOT EXISTS return_line (
  return_no        VARCHAR(40) NOT NULL,
  line_no          INT NOT NULL,
  item_id          VARCHAR(40) NOT NULL,
  qty              DECIMAL(18,3) NOT NULL,
  return_qty       DECIMAL(18,3) NOT NULL,
  restock_yn       BOOLEAN DEFAULT FALSE,
  qc_status        VARCHAR(20),
  action           VARCHAR(30),
  defect_code      VARCHAR(40),
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (return_no, line_no),
  FOREIGN KEY (return_no) REFERENCES return_header(return_no) ON DELETE CASCADE,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id),
  INDEX idx_return_no(return_no)
);

-- =====================================================
-- 12. QC_Result
-- =====================================================

CREATE TABLE IF NOT EXISTS qc_result (
  qc_id            INT AUTO_INCREMENT PRIMARY KEY,
  return_no        VARCHAR(40) NOT NULL,
  item_id          VARCHAR(40),
  result           VARCHAR(20) NOT NULL,
  action           VARCHAR(30),
  defect_code      VARCHAR(40),
  damage_ratio     DECIMAL(5,4) DEFAULT 0.5,
  memo             TEXT,
  qc_dt            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (return_no) REFERENCES return_header(return_no) ON DELETE CASCADE,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id),
  INDEX idx_return_no(return_no)
);

-- =====================================================
-- 13. Refund_Transaction
-- =====================================================

CREATE TABLE IF NOT EXISTS refund_transaction (
  refund_id        INT AUTO_INCREMENT PRIMARY KEY,
  return_no        VARCHAR(40) NOT NULL,
  so_no            VARCHAR(40),
  refund_dt        TIMESTAMP,
  refund_method    VARCHAR(30),
  amount           DECIMAL(18,2) NOT NULL,
  tax_amount       DECIMAL(18,2),
  status           VARCHAR(20) NOT NULL,
  completion_dt    TIMESTAMP,
  failure_reason   VARCHAR(200),
  bank_name        VARCHAR(40),
  account_no       VARCHAR(40),
  liability        VARCHAR(30),
  memo             TEXT,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (return_no) REFERENCES return_header(return_no),
  FOREIGN KEY (so_no) REFERENCES so_header(so_no),
  INDEX idx_return_no(return_no),
  INDEX idx_status(status),
  INDEX idx_refund_dt(refund_dt)
);

-- =====================================================
-- 14. Packaging_Work_Order
-- =====================================================

CREATE TABLE IF NOT EXISTS packaging_work_order (
  pwo_no           VARCHAR(40) PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  status           VARCHAR(30) NOT NULL,
  issued_dt        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  scheduled_dt     TIMESTAMP,
  assigned_worker  VARCHAR(40),
  pack_start_dt    TIMESTAMP,
  pack_end_dt      TIMESTAMP,
  qc_start_dt      TIMESTAMP,
  qc_end_dt        TIMESTAMP,
  qc_by            VARCHAR(40),
  FOREIGN KEY (so_no) REFERENCES so_header(so_no),
  INDEX idx_so_no(so_no),
  INDEX idx_status(status)
);

CREATE TABLE IF NOT EXISTS packaging_work_order_line (
  pwo_no           VARCHAR(40) NOT NULL,
  line_no          INT NOT NULL,
  pm_id            INT NOT NULL,
  qty_required     DECIMAL(18,3),
  qty_issued       DECIMAL(18,3),
  PRIMARY KEY (pwo_no, line_no),
  FOREIGN KEY (pwo_no) REFERENCES packaging_work_order(pwo_no) ON DELETE CASCADE,
  FOREIGN KEY (pm_id) REFERENCES packaging_material_master(pm_id)
);

-- =====================================================
-- 15. Inventory_Ledger
-- =====================================================

CREATE TABLE IF NOT EXISTS inventory_ledger (
  event_id         INT AUTO_INCREMENT PRIMARY KEY,
  event_dt         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  site             VARCHAR(50),
  loc              VARCHAR(100),
  item_id          VARCHAR(40) NOT NULL,
  lot_no           VARCHAR(40),
  qty_in           DECIMAL(18,3),
  qty_out          DECIMAL(18,3),
  ref_type         VARCHAR(30),
  ref_no           VARCHAR(40),
  damage_ratio     DECIMAL(5,4),
  memo             TEXT,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (item_id) REFERENCES item_master(item_id),
  INDEX idx_event_dt(event_dt),
  INDEX idx_item_id(item_id),
  INDEX idx_lot_no(lot_no),
  INDEX idx_ref_no(ref_no)
);

-- =====================================================
-- 16. Accounting_Journal
-- =====================================================

CREATE TABLE IF NOT EXISTS accounting_journal (
  journal_id       INT AUTO_INCREMENT PRIMARY KEY,
  event_id         INT,
  event_dt         TIMESTAMP NOT NULL,
  so_no            VARCHAR(40),
  po_no            VARCHAR(40),
  return_no        VARCHAR(40),
  transfer_no      VARCHAR(40),
  dr_acct          VARCHAR(20),
  cr_acct          VARCHAR(20),
  amount           DECIMAL(18,2) NOT NULL,
  memo             TEXT,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_event_dt(event_dt),
  INDEX idx_so_no(so_no),
  INDEX idx_return_no(return_no),
  INDEX idx_dr_acct(dr_acct),
  INDEX idx_cr_acct(cr_acct)
);

-- =====================================================
-- 17. REVENUE (매출확정) - 정상반품 3일 반영
-- =====================================================

CREATE TABLE IF NOT EXISTS revenue (
  revenue_id       VARCHAR(40) PRIMARY KEY,
  so_no            VARCHAR(40) NOT NULL,
  status           VARCHAR(30) NOT NULL,
  amount           DECIMAL(18,2) NOT NULL,
  confirmed_dt     TIMESTAMP NOT NULL,
  memo             TEXT,
  created_dt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (so_no) REFERENCES so_header(so_no),
  INDEX idx_so_no(so_no),
  INDEX idx_status(status),
  UNIQUE KEY unique_so_no(so_no)
);

-- =====================================================
-- 18. Royalty
-- =====================================================

CREATE TABLE IF NOT EXISTS royalty (
  royalty_id       INT AUTO_INCREMENT PRIMARY KEY,
  license_id       INT NOT NULL,
  calc_period_from DATE,
  calc_period_to   DATE,
  sales_qty        DECIMAL(18,3),
  royalty_amt      DECIMAL(18,2),
  payment_dt       DATE,
  status           VARCHAR(20),
  FOREIGN KEY (license_id) REFERENCES license_contract(license_id)
);

-- =====================================================
-- 19. SELECT 쿼리 (정상반품 3일 기준으로 변경)
-- =====================================================

-- 1. 정상반품 검증: 배송 완료 후 3일 이내 반품 요청 (변경됨!)
SELECT 
  so.so_no,
  so.status,
  so.delivered_dt,
  so.normal_return_due_dt,
  rh.request_dt,
  DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) as days_since_delivery,
  rh.type,
  CASE 
    WHEN DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) > 3 THEN 'INVALID: 정상반품 기한 초과 (3일)'
    WHEN rh.type = 'NORMAL' THEN 'VALID'
    ELSE 'PENDING'
  END as validation_result
FROM so_header so
JOIN return_header rh ON so.so_no = rh.so_no
WHERE so.status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND rh.type = 'NORMAL'
  AND rh.request_dt >= DATE_SUB(CURDATE(), INTERVAL 3 DAY);

-- 2. 배송 완료 후 10일 경과, 정상반품 기한(3일) 초과, 반품 미요청 건 조회 (매출확정 배치용)
-- 배송 완료 후 10일이 경과되어야 매출확정 가능하며, 그 동안 정상반품은 3일 이내만 받음
SELECT 
  so.so_no,
  so.order_dt,
  so.delivered_dt,
  so.normal_return_due_dt,
  so.return_deferral_due_dt,
  so.amount,
  so.tax,
  DATEDIFF(CURDATE(), DATE(so.delivered_dt)) as days_since_delivery
FROM so_header so
WHERE so.status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND so.revenue_confirmed_dt IS NULL
  AND DATEDIFF(CURDATE(), DATE(so.delivered_dt)) >= 10
  AND NOT EXISTS (
    SELECT 1 FROM return_header rh
    WHERE rh.so_no = so.so_no
      AND rh.status IN ('REQUESTED', 'UNDER_REVIEW', 'APPROVED')
  )
ORDER BY so.delivered_dt ASC;

-- 3. 정상반품 누적 수량 검증 (3일 기한 내)
SELECT 
  so.so_no,
  sol.line_no,
  sol.qty as order_qty,
  SUM(rl.return_qty) as total_return_qty,
  CASE 
    WHEN SUM(rl.return_qty) > sol.qty THEN 'INVALID: 누적 반품수량 초과'
    ELSE 'VALID'
  END as validation_result
FROM so_header so
JOIN so_line sol ON so.so_no = sol.so_no
JOIN return_header rh ON so.so_no = rh.so_no
  AND rh.type = 'NORMAL'
  AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) <= 3
JOIN return_line rl ON rh.return_no = rl.return_no
  AND sol.line_no = rl.line_no
WHERE rh.status IN ('APPROVED', 'ITEM_RECEIVED', 'CLOSED')
GROUP BY so.so_no, sol.line_no;

-- 4. 하자반품 검증: 배송 완료 후 7일 이내 (변경 없음)
SELECT 
  so.so_no,
  so.delivered_dt,
  rh.request_dt,
  DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) as days_since_delivery,
  CASE 
    WHEN DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) > 7 THEN 'INVALID: AS기한 만료'
    WHEN rh.type = 'DEFECT' THEN 'VALID'
    ELSE 'PENDING'
  END as validation_result
FROM so_header so
JOIN return_header rh ON so.so_no = rh.so_no
WHERE rh.type = 'DEFECT'
  AND rh.request_dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY);

-- 5. 반품 현황 (정상반품 3일 vs 하자반품 7일)
SELECT 
  so.so_no,
  so.delivered_dt,
  so.normal_return_due_dt,
  COUNT(CASE WHEN rh.type = 'NORMAL' THEN 1 END) as normal_return_count,
  COUNT(CASE WHEN rh.type = 'DEFECT' THEN 1 END) as defect_return_count,
  GROUP_CONCAT(CONCAT(rh.type, ':', rh.status) SEPARATOR ', ') as return_statuses
FROM so_header so
LEFT JOIN return_header rh ON so.so_no = rh.so_no
WHERE so.status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND so.delivered_dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY so.so_no
ORDER BY so.delivered_dt DESC;

-- 6. 배송 완료 후 10일 경과 조회 (매출확정 배치 대상)
SELECT 
  so.so_no,
  so.delivered_dt,
  DATE_ADD(so.delivered_dt, INTERVAL 10 DAY) as revenue_due_dt,
  DATEDIFF(CURDATE(), DATE(so.delivered_dt)) as days_passed,
  so.amount,
  CASE 
    WHEN DATEDIFF(CURDATE(), DATE(so.delivered_dt)) >= 10 AND so.revenue_confirmed_dt IS NULL THEN '매출확정 대상'
    WHEN so.revenue_confirmed_dt IS NOT NULL THEN '이미 매출확정'
    ELSE '대기 중'
  END as status_label
FROM so_header so
WHERE so.status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND so.delivered_dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY so.delivered_dt ASC;

-- 7. 정상반품 vs 하자반품 기한 비교
SELECT 
  rh.return_no,
  rh.so_no,
  rh.type,
  so.delivered_dt,
  rh.request_dt,
  DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) as days_since_delivery,
  CASE 
    WHEN rh.type = 'NORMAL' AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) <= 3 THEN '정상반품 유효 (3일 이내)'
    WHEN rh.type = 'NORMAL' AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) > 3 THEN '정상반품 만료 (3일 초과)'
    WHEN rh.type = 'DEFECT' AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) <= 7 THEN 'AS 유효 (7일 이내)'
    WHEN rh.type = 'DEFECT' AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) > 7 THEN 'AS 만료 (7일 초과)'
    ELSE 'Unknown'
  END as return_status
FROM return_header rh
JOIN so_header so ON rh.so_no = so.so_no
WHERE rh.request_dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY rh.request_dt DESC;

-- =====================================================
-- 20. 인덱스 최적화 (정상반품 3일 기준)
-- =====================================================

-- 정상반품 3일 기한 조회 인덱스
CREATE INDEX idx_so_normal_return_validation ON so_header(delivered_dt, normal_return_due_dt)
WHERE status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED');

-- 매출확정 10일 기한 조회 인덱스
CREATE INDEX idx_so_revenue_confirmation_modified ON so_header(status, revenue_confirmed_dt, delivered_dt)
WHERE status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED')
  AND revenue_confirmed_dt IS NULL;

-- Return_Header 타입별 검색 인덱스
CREATE INDEX idx_return_type_request ON return_header(type, request_dt, so_no);

-- 반품 기한 검증 인덱스
CREATE INDEX idx_return_validation_dates ON so_header(delivered_dt, status)
WHERE status IN ('DELIVERED', 'PARTIALLY_DELIVERED', 'COMPLETED');

-- =====================================================
-- 21. 마이그레이션 스크립트 (기존 데이터 전환)
-- =====================================================

-- SO_Header 정상반품 기한 필드 추가 및 초기화
ALTER TABLE so_header
ADD COLUMN IF NOT EXISTS normal_return_due_dt TIMESTAMP,
ADD COLUMN IF NOT EXISTS return_deferral_due_dt TIMESTAMP;

-- 기존 배송 완료 데이터에 대해 정상반품(3일) 및 매출확정(10일) 기한 계산
UPDATE so_header 
SET normal_return_due_dt = DATE_ADD(delivered_dt, INTERVAL 3 DAY),
    return_deferral_due_dt = DATE_ADD(delivered_dt, INTERVAL 10 DAY)
WHERE delivered_dt IS NOT NULL 
  AND (normal_return_due_dt IS NULL OR return_deferral_due_dt IS NULL);

-- 기존 반품이 있는 경우, 정상반품 기한 확인
UPDATE return_header rh
SET rh.status = 'REJECTED'
WHERE rh.type = 'NORMAL'
  AND rh.status IN ('REQUESTED', 'UNDER_REVIEW')
  AND EXISTS (
    SELECT 1 FROM so_header so
    WHERE so.so_no = rh.so_no
      AND DATEDIFF(DATE(rh.request_dt), DATE(so.delivered_dt)) > 3
  );

-- =====================================================
-- 22. 주석 및 설명
-- =====================================================

/**
  정상반품 기한 변경 사항 정리
  
  [변경 전 (V2.5)]
  - 정상반품: 배송 완료 후 10일 이내
  - 하자반품: 배송 완료 후 7일 이내
  - 매출확정: 배송 완료 후 10일 이상 경과
  
  [변경 후 (V2.5-Modified)]
  - 정상반품: 배송 완료 후 3일 이내 (더 엄격함)
  - 하자반품: 배송 완료 후 7일 이내 (유지)
  - 매출확정: 배송 완료 후 10일 이상 경과 (유지)
  
  [주요 SQL 변경 항목]
  1. SO_Header에 두 개 필드 추가:
     - normal_return_due_dt: 정상반품 기한 (배송 완료 + 3일)
     - return_deferral_due_dt: 매출확정 기한 (배송 완료 + 10일)
  
  2. 검증 로직:
     - 정상반품: DATEDIFF(request_dt - delivered_dt) <= 3
     - 하자반품: DATEDIFF(request_dt - delivered_dt) <= 7
     - 매출확정: DATEDIFF(current_date - delivered_dt) >= 10
  
  3. 반품 기한 기준:
     - 정상반품이 3일을 초과하면 자동 거부 가능
     - 하자반품은 여전히 7일 유지
  
  4. 인덱스 최적화:
     - normal_return_due_dt, return_deferral_due_dt 인덱스 추가
     - 기한 검증 쿼리 성능 개선
 */

-- =====================================================
-- END OF V2.5-MODIFIED SQL SCRIPT (정상반품 3일)
-- =====================================================
