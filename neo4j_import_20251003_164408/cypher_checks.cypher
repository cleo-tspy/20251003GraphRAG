// 1) 某訂單的工單與回報數量
MATCH (o:SalesOrder {orderNo: 'S11-241100009'})-[:HAS_ITEM]->(i)-[:CREATES]->(w:WorkOrder)-[:HAS_STEP]->(s)
OPTIONAL MATCH (s)-[:REPORTED_BY]->(r:ProductionReport)
RETURN o.orderNo, w.workOrderNo, sum(r.qty) AS reportedQty;

// 2) 最近7天未回報的步驟
MATCH (s:WorkOrderStep)
WHERE NOT EXISTS { MATCH (s)-[:REPORTED_BY]->(r:ProductionReport) WHERE r.reportTime >= datetime() - duration('P7D') }
RETURN s.stepId, s.workOrderNo, s.seq
LIMIT 50;

// 3) 依工作中心彙總的回報
MATCH (wc:WorkCenter)<-[:AT]-(s:WorkOrderStep)-[:REPORTED_BY]->(r:ProductionReport)
RETURN wc.workCenterId, sum(r.qty) AS qty7d
ORDER BY qty7d DESC;
