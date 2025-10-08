// === Constraints / Indexes (Neo4j 5+) ===
CREATE CONSTRAINT salesorder_pk IF NOT EXISTS FOR (n:SalesOrder) REQUIRE n.orderNo IS UNIQUE;
CREATE CONSTRAINT salesorderitem_pk IF NOT EXISTS FOR (n:SalesOrderItem) REQUIRE n.orderItemId IS UNIQUE;
CREATE CONSTRAINT part_pk IF NOT EXISTS FOR (n:Part) REQUIRE n.partNo IS UNIQUE;
CREATE CONSTRAINT workorder_pk IF NOT EXISTS FOR (n:WorkOrder) REQUIRE n.workOrderNo IS UNIQUE;
CREATE CONSTRAINT workcenter_pk IF NOT EXISTS FOR (n:WorkCenter) REQUIRE n.workCenterId IS UNIQUE;
CREATE CONSTRAINT productionreport_pk IF NOT EXISTS FOR (n:ProductionReport) REQUIRE n.reportId IS UNIQUE;

// event
CREATE CONSTRAINT et_name IF NOT EXISTS FOR (t:EventType)      REQUIRE t.name IS UNIQUE;
CREATE CONSTRAINT ev_id   IF NOT EXISTS FOR (e:Event)          REQUIRE e.eventId IS UNIQUE;
CREATE CONSTRAINT th_id   IF NOT EXISTS FOR (t:Thread)         REQUIRE t.threadId IS UNIQUE;
CREATE CONSTRAINT wo_id   IF NOT EXISTS FOR (w:WorkOrder)      REQUIRE w.workOrderId IS UNIQUE;
CREATE CONSTRAINT seg_id  IF NOT EXISTS FOR (s:ProductSegment) REQUIRE s.segmentId IS UNIQUE;
CREATE CONSTRAINT ag_id   IF NOT EXISTS FOR (a:Agent)          REQUIRE a.agentId IS UNIQUE;
CREATE CONSTRAINT sys_nm  IF NOT EXISTS FOR (s:System)         REQUIRE s.name IS UNIQUE;
CREATE CONSTRAINT ogr_id  IF NOT EXISTS FOR (d:OutsourcedGoodsReceipt) REQUIRE d.ogrId IS UNIQUE;

CREATE INDEX ev_type IF NOT EXISTS FOR (e:Event) ON (e.eventType);
CREATE INDEX ev_time IF NOT EXISTS FOR (e:Event) ON (e.occurredAt);