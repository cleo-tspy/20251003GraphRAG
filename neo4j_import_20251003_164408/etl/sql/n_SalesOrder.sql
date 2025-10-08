SELECT DISTINCT
  so.orderNo AS salesOrderId,
  so.orderNo AS orderNo,
  MIN(so.createDatetime) AS orderDate,
  MIN(so.orderDueDate) AS orderRequestedDueDate,
  'TWD' AS currency,              -- 全部預設為 TWD
  MIN(so.customer) AS customerId,
  'confirmed' AS salesOrderStatus -- 全部預設為 confirmed
FROM lean.salesorder AS so
WHERE so.orderDueDate >= :from
  AND so.orderDueDate <  :to
GROUP BY so.orderNo
