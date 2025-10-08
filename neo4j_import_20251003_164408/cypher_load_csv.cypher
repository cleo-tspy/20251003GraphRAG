// Place CSVs in $NEO4J_HOME/import (or set dbms.directories.import)
// Run these in Neo4j Browser or cypher-shell:
//
// :source cypher_setup.cypher
// :source cypher_load_csv.cypher
// :source cypher_checks.cypher
//
// This script is aligned to the current repo structure:
// - e_eventType.csv / e_triggers.csv
// - e_event.csv  (single file; carries all event relations via fields)
// - nodes/*      (static nodes)
// - relations/*  (static non-event relationships)

////////////////////////////////////////////////////////////////////////
// 0) Intent (EventType & TRIGGERS)
////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM 'file:///e_eventType.csv' AS row
WITH row WHERE row.name IS NOT NULL AND row.name <> ''
MERGE (et:EventType {name: trim(row.name)})
SET et.label_zh_tw = coalesce(row.label_zh_tw, et.label_zh_tw),
    et.description = coalesce(row.description, et.description);

LOAD CSV WITH HEADERS FROM 'file:///e_triggers.csv' AS row
WITH row WHERE row.fromType IS NOT NULL AND row.toType IS NOT NULL
MATCH (from:EventType {name: trim(row.fromType)})
MATCH (to:EventType   {name: trim(row.toType)})
MERGE (from)-[r:TRIGGERS]->(to)
SET r.guard    = coalesce(row.guard,''),
    r.guard_zh = coalesce(row.guard_zh,''),
    r.notes    = coalesce(row.notes,'');

////////////////////////////////////////////////////////////////////////
// 1) Nodes (static)
////////////////////////////////////////////////////////////////////////

/** Helper: COALESCE likely id fields per node type
 *  We try common header names to avoid tight coupling to CSV schemas.
 */

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_Part.csv' AS row
WITH row, coalesce(row.partId,row.partNo,row.id) AS pid
WHERE pid IS NOT NULL AND pid <> ''
MERGE (p:Part {partId: trim(pid)})
SET p.name = coalesce(row.name,row.partName,p.name),
    p.uom  = coalesce(row.uom,p.uom);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_BOM.csv' AS row
WITH row, coalesce(row.bomId,row.id) AS bid
WHERE bid IS NOT NULL AND bid <> ''
MERGE (b:BOM {bomId: trim(bid)})
SET b.version = coalesce(row.version,b.version),
    b.siteId  = coalesce(row.siteId,b.siteId),
    b.note    = coalesce(row.note,b.note);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_BOMComponent.csv' AS row
WITH row, coalesce(row.bomComponentId,row.componentId,row.id) AS bcid
WHERE bcid IS NOT NULL AND bcid <> ''
MERGE (bc:BOMComponent {bomComponentId: trim(bcid)})
SET bc.quantity      = CASE WHEN coalesce(row.quantity,'')='' THEN bc.quantity ELSE toFloat(row.quantity) END,
    bc.uom           = coalesce(row.uom,bc.uom),
    bc.effectiveFrom = coalesce(row.effectiveFrom,row.bomEffectiveFrom,bc.effectiveFrom),
    bc.effectiveTo   = coalesce(row.effectiveTo,row.bomEffectiveTo,bc.effectiveTo),
    bc.sequence      = CASE WHEN coalesce(row.sequence,'')='' THEN bc.sequence ELSE toInteger(row.sequence) END;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ProductSegment.csv' AS row
WITH row, coalesce(row.segmentId,row.productSegmentId,row.id) AS sid
WHERE sid IS NOT NULL AND sid <> ''
MERGE (s:ProductSegment {segmentId: trim(sid)})
SET s.sequence = CASE WHEN coalesce(row.sequence,'')='' THEN s.sequence ELSE toInteger(row.sequence) END,
    s.name     = coalesce(row.name,row.segmentName,s.name);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ProductionRouting.csv' AS row
WITH row, coalesce(row.productionRoutingId,row.routingId,row.id) AS rid
WHERE rid IS NOT NULL AND rid <> ''
MERGE (rt:ProductionRouting {productionRoutingId: trim(rid)})
SET rt.version = coalesce(row.version,rt.version),
    rt.siteId  = coalesce(row.siteId,rt.siteId);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_Process.csv' AS row
WITH row, coalesce(row.processId,row.id) AS pid2
WHERE pid2 IS NOT NULL AND pid2 <> ''
MERGE (pr:Process {processId: trim(pid2)})
SET pr.name = coalesce(row.name,pr.name);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_WorkOrder.csv' AS row
WITH row, coalesce(row.workOrderId,row.workOrderNo,row.woId,row.id) AS wid
WHERE wid IS NOT NULL AND wid <> ''
MERGE (w:WorkOrder {workOrderId: trim(wid)})
SET w.status    = coalesce(row.status,w.status),
    w.partId    = coalesce(row.partId,row.partNo,w.partId),
    w.qtyPlanned= CASE WHEN coalesce(row.qtyPlanned,'')='' THEN w.qtyPlanned ELSE toFloat(row.qtyPlanned) END,
    w.startDate = CASE WHEN coalesce(row.startDate,'')='' THEN w.startDate ELSE date(row.startDate) END,
    w.dueDate   = CASE WHEN coalesce(row.dueDate,'')='' THEN w.dueDate ELSE date(row.dueDate) END;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_SalesOrder.csv' AS row
WITH row, coalesce(row.salesOrderId,row.orderNo,row.id) AS soid
WHERE soid IS NOT NULL AND soid <> ''
MERGE (so:SalesOrder {salesOrderId: trim(soid)})
SET so.orderDate   = CASE WHEN coalesce(row.orderDate,'')='' THEN so.orderDate ELSE date(row.orderDate) END,
    so.customerCode= coalesce(row.customerCode,so.customerCode);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_SalesOrderItem.csv' AS row
WITH row, coalesce(row.salesOrderItemId,row.orderItemId,row.itemId,row.id) AS soi
WHERE soi IS NOT NULL AND soi <> ''
MERGE (si:SalesOrderItem {salesOrderItemId: trim(soi)})
SET si.lineNo     = CASE WHEN coalesce(row.lineNo,'')='' THEN si.lineNo ELSE toInteger(row.lineNo) END,
    si.partId     = coalesce(row.partId,row.partNo,si.partId),
    si.qtyOrdered = CASE WHEN coalesce(row.qtyOrdered,'')='' THEN si.qtyOrdered ELSE toFloat(row.qtyOrdered) END,
    si.dueDate    = CASE WHEN coalesce(row.dueDate,'')='' THEN si.dueDate ELSE date(row.dueDate) END;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ProductionKanban.csv' AS row
WITH row, coalesce(row.productionKanbanId,row.kanbanId,row.id) AS pkid
WHERE pkid IS NOT NULL AND pkid <> ''
MERGE (pk:ProductionKanban {productionKanbanId: trim(pkid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_WorkCenter.csv' AS row
WITH row, coalesce(row.workCenterId,row.id) AS wcid
WHERE wcid IS NOT NULL AND wcid <> ''
MERGE (wc:WorkCenter {workCenterId: trim(wcid)})
SET wc.name = coalesce(row.name,wc.name),
    wc.department = coalesce(row.department,wc.department);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_Operator.csv' AS row
WITH row, coalesce(row.operatorId,row.agentId,row.id) AS oid
WHERE oid IS NOT NULL AND oid <> ''
MERGE (op:Operator {operatorId: trim(oid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_WarehouseClerk.csv' AS row
WITH row, coalesce(row.warehouseClerkId,row.agentId,row.id) AS cid
WHERE cid IS NOT NULL AND cid <> ''
MERGE (wc:WarehouseClerk {warehouseClerkId: trim(cid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_Subcontractor.csv' AS row
WITH row, coalesce(row.subcontractorId,row.agentId,row.id) AS scid
WHERE scid IS NOT NULL AND scid <> ''
MERGE (sc:Subcontractor {subcontractorId: trim(scid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_System.csv' AS row
WITH row, coalesce(row.name,row.systemName,row.id) AS sysname
WHERE sysname IS NOT NULL AND sysname <> ''
MERGE (sys:System {name: trim(sysname)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ProductionReport.csv' AS row
WITH row, coalesce(row.productionReportId,row.reportId,row.id) AS prid
WHERE prid IS NOT NULL AND prid <> ''
MERGE (prr:ProductionReport {productionReportId: trim(prid)})
SET prr.qty = CASE WHEN coalesce(row.qty,'')='' THEN prr.qty ELSE toFloat(row.qty) END;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ActualDataBox.csv' AS row
WITH row, coalesce(row.actualDataBoxId,row.id) AS adid
WHERE adid IS NOT NULL AND adid <> ''
MERGE (adb:ActualDataBox {actualDataBoxId: trim(adid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_OperationsSchedule.csv' AS row
WITH row, coalesce(row.operationsScheduleId,row.id) AS osid
WHERE osid IS NOT NULL AND osid <> ''
MERGE (os:OperationsSchedule {operationsScheduleId: trim(osid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_ProcessDataBox.csv' AS row
WITH row, coalesce(row.processDataBoxId,row.id) AS pdbid
WHERE pdbid IS NOT NULL AND pdbid <> ''
MERGE (pdb:ProcessDataBox {processDataBoxId: trim(pdbid)});

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes/n_OutsourcedGoodsReceipt.csv' AS row
WITH row, coalesce(row.ogrId,row.docId,row.id) AS ogr
WHERE ogr IS NOT NULL AND ogr <> ''
MERGE (d:OutsourcedGoodsReceipt {ogrId: trim(ogr)});

////////////////////////////////////////////////////////////////////////
// 2) Relations (static, non-event)
////////////////////////////////////////////////////////////////////////

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_part_hasBOM_BOM.csv' AS row
WITH coalesce(row.partId,row.partNo,row.id) AS pid, coalesce(row.bomId,row.id2) AS bid
WHERE pid IS NOT NULL AND bid IS NOT NULL AND pid<>'' AND bid<>''
MATCH (p:Part {partId: trim(pid)})
MATCH (b:BOM  {bomId:  trim(bid)})
MERGE (p)-[:HAS_BOM]->(b);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_BOM_includesComponent_BOMComponent.csv' AS row
WITH coalesce(row.bomId,row.id) AS bid, coalesce(row.bomComponentId,row.componentId,row.id2) AS bcid
WHERE bid IS NOT NULL AND bcid IS NOT NULL AND bid<>'' AND bcid<>''
MATCH (b:BOM {bomId: trim(bid)})
MATCH (bc:BOMComponent {bomComponentId: trim(bcid)})
MERGE (b)-[:INCLUDES_COMPONENT]->(bc);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_BOMComponent_consumesChildPart_Part.csv' AS row
WITH coalesce(row.bomComponentId,row.componentId,row.id) AS bcid, coalesce(row.childPartId,row.partId,row.partNo,row.id2) AS cid
WHERE bcid IS NOT NULL AND cid IS NOT NULL AND bcid<>'' AND cid<>''
MATCH (bc:BOMComponent {bomComponentId: trim(bcid)})
MATCH (p:Part {partId: trim(cid)})
MERGE (bc)-[:CONSUMES_CHILD_PART]->(p);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionRouting_hasOrderedSegment_ProductSegment.csv' AS row
WITH coalesce(row.productionRoutingId,row.routingId,row.id) AS rid,
     coalesce(row.segmentId,row.productSegmentId,row.id2) AS sid,
     coalesce(row.sequence,row.seq) AS seq
WHERE rid IS NOT NULL AND sid IS NOT NULL AND rid<>'' AND sid<>''
MATCH (rt:ProductionRouting {productionRoutingId: trim(rid)})
MATCH (s:ProductSegment {segmentId: trim(sid)})
MERGE (rt)-[r:HAS_ORDERED_SEGMENT]->(s)
ON CREATE SET r.sequence = CASE WHEN coalesce(seq,'')='' THEN NULL ELSE toInteger(seq) END;

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductSegment_realizes_Process.csv' AS row
WITH coalesce(row.segmentId,row.productSegmentId,row.id) AS sid, coalesce(row.processId,row.id2) AS pid2
WHERE sid IS NOT NULL AND pid2 IS NOT NULL AND sid<>'' AND pid2<>''
MATCH (s:ProductSegment {segmentId: trim(sid)})
MATCH (p:Process {processId: trim(pid2)})
MERGE (s)-[:REALIZES]->(p);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductSegment_hasActualDataBox_ActualDataBox.csv' AS row
WITH coalesce(row.segmentId,row.productSegmentId,row.id) AS sid, coalesce(row.actualDataBoxId,row.id2) AS adid
WHERE sid IS NOT NULL AND adid IS NOT NULL AND sid<>'' AND adid<>''
MATCH (s:ProductSegment {segmentId: trim(sid)})
MATCH (adb:ActualDataBox {actualDataBoxId: trim(adid)})
MERGE (s)-[:HAS_ACTUAL_DATA_BOX]->(adb);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_Part_hasProductionRouting_ProductionRouting.csv' AS row
WITH coalesce(row.partId,row.partNo,row.id) AS pid, coalesce(row.productionRoutingId,row.routingId,row.id2) AS rid
WHERE pid IS NOT NULL AND rid IS NOT NULL AND pid<>'' AND rid<>''
MATCH (p:Part {partId: trim(pid)})
MATCH (rt:ProductionRouting {productionRoutingId: trim(rid)})
MERGE (p)-[:HAS_PRODUCTION_ROUTING]->(rt);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_SalesOrder_hasItem_SalesOrderItem.csv' AS row
WITH coalesce(row.salesOrderId,row.orderNo,row.id) AS soid, coalesce(row.salesOrderItemId,row.orderItemId,row.itemId,row.id2) AS soi
WHERE soid IS NOT NULL AND soi IS NOT NULL AND soid<>'' AND soi<>''
MATCH (so:SalesOrder {salesOrderId: trim(soid)})
MATCH (si:SalesOrderItem {salesOrderItemId: trim(soi)})
MERGE (so)-[:HAS_ITEM]->(si);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_SalesOrderItem_createsWorkOrder_WorkOrder.csv' AS row
WITH coalesce(row.salesOrderItemId,row.orderItemId,row.itemId,row.id) AS soi, coalesce(row.workOrderId,row.workOrderNo,row.woId,row.id2) AS wid
WHERE soi IS NOT NULL AND wid IS NOT NULL AND soi<>'' AND wid<>''
MATCH (si:SalesOrderItem {salesOrderItemId: trim(soi)})
MATCH (w:WorkOrder {workOrderId: trim(wid)})
MERGE (si)-[:CREATES_WORK_ORDER]->(w);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_SalesOrderItem_requiredPart_Part.csv' AS row
WITH coalesce(row.salesOrderItemId,row.orderItemId,row.itemId,row.id) AS soi, coalesce(row.partId,row.partNo,row.id2) AS pid
WHERE soi IS NOT NULL AND pid IS NOT NULL AND soi<>'' AND pid<>''
MATCH (si:SalesOrderItem {salesOrderItemId: trim(soi)})
MATCH (p:Part {partId: trim(pid)})
MERGE (si)-[:REQUIRED_PART]->(p);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_OperationsSchedule_hasProductionKanban_ProductionKanban.csv' AS row
WITH coalesce(row.operationsScheduleId,row.id) AS osid, coalesce(row.productionKanbanId,row.kanbanId,row.id2) AS pkid
WHERE osid IS NOT NULL AND pkid IS NOT NULL AND osid<>'' AND pkid<>''
MATCH (os:OperationsSchedule {operationsScheduleId: trim(osid)})
MATCH (pk:ProductionKanban {productionKanbanId: trim(pkid)})
MERGE (os)-[:HAS_PRODUCTION_KANBAN]->(pk);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionKanban_belongsToWorkOrder_WorkOrder.csv' AS row
WITH coalesce(row.productionKanbanId,row.kanbanId,row.id) AS pkid, coalesce(row.workOrderId,row.workOrderNo,row.woId,row.id2) AS wid
WHERE pkid IS NOT NULL AND wid IS NOT NULL AND pkid<>'' AND wid<>''
MATCH (pk:ProductionKanban {productionKanbanId: trim(pkid)})
MATCH (w:WorkOrder {workOrderId: trim(wid)})
MERGE (pk)-[:BELONGS_TO_WORK_ORDER]->(w);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionKanban_executesProductSegment_ProductSegment.csv' AS row
WITH coalesce(row.productionKanbanId,row.kanbanId,row.id) AS pkid, coalesce(row.segmentId,row.productSegmentId,row.id2) AS sid
WHERE pkid IS NOT NULL AND sid IS NOT NULL AND pkid<>'' AND sid<>''
MATCH (pk:ProductionKanban {productionKanbanId: trim(pkid)})
MATCH (s:ProductSegment {segmentId: trim(sid)})
MERGE (pk)-[:EXECUTES_PRODUCT_SEGMENT]->(s);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionKanban_scheduledAtWorkCenter_WorkCenter.csv' AS row
WITH coalesce(row.productionKanbanId,row.kanbanId,row.id) AS pkid, coalesce(row.workCenterId,row.id2) AS wcid
WHERE pkid IS NOT NULL AND wcid IS NOT NULL AND pkid<>'' AND wcid<>''
MATCH (pk:ProductionKanban {productionKanbanId: trim(pkid)})
MATCH (wc:WorkCenter {workCenterId: trim(wcid)})
MERGE (pk)-[:SCHEDULED_AT_WORK_CENTER]->(wc);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionReport_forProductSegment_ProductSegment.csv' AS row
WITH coalesce(row.productionReportId,row.reportId,row.id) AS prid, coalesce(row.segmentId,row.productSegmentId,row.id2) AS sid
WHERE prid IS NOT NULL AND sid IS NOT NULL AND prid<>'' AND sid<>''
MATCH (prr:ProductionReport {productionReportId: trim(prid)})
MATCH (s:ProductSegment {segmentId: trim(sid)})
MERGE (prr)-[:FOR_PRODUCT_SEGMENT]->(s);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionReport_performedBy_Operator.csv' AS row
WITH coalesce(row.productionReportId,row.reportId,row.id) AS prid, coalesce(row.operatorId,row.agentId,row.id2) AS oid
WHERE prid IS NOT NULL AND oid IS NOT NULL AND prid<>'' AND oid<>''
MATCH (prr:ProductionReport {productionReportId: trim(prid)})
MATCH (op:Operator {operatorId: trim(oid)})
MERGE (prr)-[:PERFORMED_BY]->(op);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///relations/r_ProductionReport_relatesTo_ProductionKanban.csv' AS row
WITH coalesce(row.productionReportId,row.reportId,row.id) AS prid, coalesce(row.productionKanbanId,row.kanbanId,row.id2) AS pkid
WHERE prid IS NOT NULL AND pkid IS NOT NULL AND prid<>'' AND pkid<>''
MATCH (prr:ProductionReport {productionReportId: trim(prid)})
MATCH (pk:ProductionKanban {productionKanbanId: trim(pkid)})
MERGE (prr)-[:RELATES_TO]->(pk);

////////////////////////////////////////////////////////////////////////
// 3) Events (single e_event.csv) + derived edges
////////////////////////////////////////////////////////////////////////

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///e_event.csv' AS row
WITH
  trim(row.eventId)        AS eid,
  trim(row.eventType)      AS etype,
  trim(row.occurredAt)     AS ts,
  trim(row.threadId)       AS tid,
  trim(row.workOrderId)    AS wid,
  trim(row.productSegmentId) AS sid,
  trim(row.agentId)        AS aid,
  trim(row.agentRole)      AS arole,
  trim(row.sourceSystem)   AS sysname,
  trim(row.docId)          AS docid,
  row.attrs                AS attrs
WHERE eid <> '' AND etype <> '' AND ts <> '' AND tid <> ''
MERGE (e:Event {eventId: eid})
ON CREATE SET
  e.eventType  = etype,
  e.occurredAt = datetime(ts),
  e.threadId   = tid
ON MATCH SET
  e.eventType  = coalesce(e.eventType,etype),
  e.occurredAt = coalesce(e.occurredAt,datetime(ts)),
  e.threadId   = coalesce(e.threadId,tid)

// Attach Thread
MERGE (t:Thread {threadId: tid})
MERGE (e)-[:IN_THREAD]->(t)

// WorkOrder (optional)
FOREACH (_ IN CASE WHEN wid IS NOT NULL AND wid <> '' THEN [1] ELSE [] END |
  MERGE (w:WorkOrder {workOrderId: wid})
  MERGE (e)-[:RELATES_TO]->(w)
)

// ProductSegment (optional)
FOREACH (_ IN CASE WHEN sid IS NOT NULL AND sid <> '' THEN [1] ELSE [] END |
  MERGE (s:ProductSegment {segmentId: sid})
  MERGE (e)-[:AT_SEGMENT]->(s)
)

// Agent (generic, optional)
FOREACH (_ IN CASE WHEN aid IS NOT NULL AND aid <> '' THEN [1] ELSE [] END |
  MERGE (a:Agent {agentId: aid})
  ON CREATE SET a.role = arole
  MERGE (e)-[:PERFORMED_BY]->(a)
)

// System recordedIn (optional)
FOREACH (_ IN CASE WHEN sysname IS NOT NULL AND sysname <> '' THEN [1] ELSE [] END |
  MERGE (sys:System {name: sysname})
  MERGE (e)-[:RECORDED_IN]->(sys)
)

// Docs (OGR) based on eventType + docId
FOREACH (_ IN CASE WHEN docid IS NOT NULL AND docid <> '' AND etype = 'GenerateOutsourcedGoodsReceipt' THEN [1] ELSE [] END |
  MERGE (d:OutsourcedGoodsReceipt {ogrId: docid})
  MERGE (e)-[:PRODUCES]->(d)
)
FOREACH (_ IN CASE WHEN docid IS NOT NULL AND docid <> '' AND etype = 'ReceiveOutsourcedToWarehouse' THEN [1] ELSE [] END |
  MERGE (d:OutsourcedGoodsReceipt {ogrId: docid})
  MERGE (e)-[:USES]->(d)
)

// attrs JSON merge (requires APOC; if not available, comment this block)
WITH e, attrs
CALL apoc.do.when(
  attrs IS NOT NULL AND attrs <> '',
  'SET e += apoc.convert.fromJsonMap($m) RETURN e',
  'RETURN e',
  {m: attrs, e: e}
) YIELD value
RETURN count(e);

////////////////////////////////////////////////////////////////////////
// 4) Link :NEXT within each thread (occurredAt, tie-breaker eventId)
////////////////////////////////////////////////////////////////////////

// Remove existing NEXT to rebuild idempotently
MATCH ()-[r:NEXT]->() DELETE r;

MATCH (t:Thread)<-[:IN_THREAD]-(e:Event)
WITH t, e
ORDER BY t.threadId, e.occurredAt, e.eventId
WITH t, collect(e) AS evs
UNWIND range(0, size(evs)-2) AS i
WITH evs[i] AS a, evs[i+1] AS b
MERGE (a)-[n:NEXT]->(b)
SET n.cause = CASE
  WHEN EXISTS {
    MATCH (:EventType {name:a.eventType})-[:TRIGGERS]->(:EventType {name:b.eventType})
  } THEN 'triggers' ELSE 'unspecified' END;
