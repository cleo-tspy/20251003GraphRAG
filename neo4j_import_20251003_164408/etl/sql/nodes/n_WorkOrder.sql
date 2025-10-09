-- 依賴：ids_cte(salesOrderItemId) 由 runner 透過 cte_from_csv 注入
SELECT
  kpi.WorkOrderNo AS workOrderId,
  kpi.WorkOrderNo AS workOrderNo,
  CASE
    WHEN NULLIF(kpi.ActualStartTime,'0000-00-00 00:00:00') IS NULL THEN 'released'
    WHEN NULLIF(kpi.ActualEndTime,'0000-00-00 00:00:00')   IS NULL THEN 'in_progress'
    ELSE 'completed'
  END AS workOrderStatus,
  kpi.RequestQuantity AS workOrderQuantityPlanned
FROM kanban_produceinstruction kpi
JOIN ids_cte t ON t.salesOrderItemId = kpi.OrderOid
WHERE kpi.ScheduleType = 2
  AND kpi.IsMarkAsDelete = 'N'
