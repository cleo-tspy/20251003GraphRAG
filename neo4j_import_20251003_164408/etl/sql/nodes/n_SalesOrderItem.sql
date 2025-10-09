SELECT DISTINCT
  so.oid AS salesOrderItemId,
  so.seqNo AS seqNo,
  productNo AS partId,
  orderQty AS requiredQuantity,
  orderDueDate AS itemRequestedDueDate
FROM lean.salesorder AS so
WHERE so.orderDueDate >= :from
  AND so.orderDueDate <  :to