# Neo4j Import Templates — README

---

## Quick Start

1. 將整個資料夾的 CSV 複製到 `$NEO4J_HOME/import/`
2. 在 Neo4j Browser 依序執行：
   ```
   :source cypher_setup.cypher     // 建立索引與唯一鍵
   :source cypher_load_csv.cypher  // 載入 nodes、relations、intent（EventType/Triggers）、events、串接 NEXT
   :source cypher_checks.cypher    // 基本檢核
   ```
3. 完成後，你可以用以下範例查詢驗收：
   ```cypher
   // 看某 thread 的事件順序
   MATCH (t:Thread {threadId:$threadId})<-[:IN_THREAD]-(e:Event)
   RETURN e.eventId, e.eventType, e.occurredAt ORDER BY e.occurredAt, e.eventId;
   ```
---

## 資料夾結構

```
.
├── cypher_setup.cypher         # 索引/唯一鍵
├── cypher_load_csv.cypher      # 載入全部 CSV（含建立 :NEXT）
├── cypher_checks.cypher        # 匯入後檢核
├── e_eventType.csv             # Intent：事件型別
├── e_triggers.csv              # Intent：型別間觸發關係與 guard
├── e_event.csv                 # Trace：事件本體（單檔，見下方欄位契約）
├── nodes/                      # 各類節點 CSV
└── edges/                  # 各類關係 CSV（不含事件；事件關係由 e_event.csv 產生）
```

---

## `e_event.csv` 欄位契約

**欄位順序：**
```
eventId,eventType,occurredAt,threadId,workOrderId,productSegmentId,agentId,agentRole,sourceSystem,docId,attrs
```

| 欄位 | 必填 | 說明（匯入時的語意與落地） |
|---|---|---|
| `eventId` | ✅ | 事件唯一鍵 → `(:Event {eventId})` |
| `eventType` | ✅ | 事件型別（需存在於 `e_eventType.csv`）→ `Event.eventType`；也會用於 `TRIGGERS` 檢核 |
| `occurredAt` | ✅ | ISO8601（建議 UTC，如 `2025-10-07T01:23:45Z`）。同一 `threadId` 內依此建立 `(:Event)-[:NEXT]->(:Event)`；同秒時以 `eventId` 當次序 tie-breaker |
| `threadId` | ✅ | 用來分群串接 `:NEXT` 的主線鍵（建議 `WO#SEG` 或你的規則）。匯入時建立 `(:Thread {threadId})` 並連 `(:Event)-[:IN_THREAD]->(:Thread)` |
| `workOrderId` | ❶ | 若有 → `(:Event)-[:RELATES_TO]->(:WorkOrder {workOrderId})` |
| `productSegmentId` | ❶ | 若有 → `(:Event)-[:AT_SEGMENT]->(:ProductSegment {segmentId})` |
| `agentId` | ❶ | 若有 → `(:Agent {agentId, role:agentRole})`，並建立 `(:Event)-[:PERFORMED_BY]->(:Agent)` |
| `agentRole` | ❶ | 與 `agentId` 搭配（Operator / WarehouseClerk / Subcontractor / ERPSystem…） |
| `sourceSystem` | ❶ | 若有 → `(:System {name})`，並建立 `(:Event)-[:RECORDED_IN]->(:System)` |
| `docId` | ❶ | 若有，並且 `eventType` 是 **GenerateOutsourcedGoodsReceipt** → 建立 `(:OutsourcedGoodsReceipt {ogrId:docId})` 與 `(:Event)-[:PRODUCES]->(OGR)`；若 `eventType` 是 **ReceiveOutsourcedToWarehouse** → 建立 `(:Event)-[:USES]->(OGR)`；其他型別忽略 |
| `attrs` | ❶ | **JSON 字串**（型別專屬屬性，會合併進事件節點，例如 `{"isFinalSegment":true,"isOutsourced":false,"reportQty":10}`）。未知鍵允許；若需嚴格檢核，請在 `cypher_checks.cypher` 開啟對應段落 |

> ❶ = 可空；空字串將被略過，不建立對應邊或節點。

**範例列：**
```csv
eventId,eventType,occurredAt,threadId,workOrderId,productSegmentId,agentId,agentRole,sourceSystem,docId,attrs
E2,OperationCheckOut,2025-10-07T02:00:00Z,WO123#PS_A_20,WO123,PS_A_20,U001,Operator,MES,,"{""isFinalSegment"":true,""reportQty"":10}"
E8,GenerateOutsourcedGoodsReceipt,2025-10-07T01:11:00Z,WO888,WO888,,SYS,ERPSystem,ERP,OGR-5566,"{}"
E9,ReceiveOutsourcedToWarehouse,2025-10-07T02:20:00Z,WO888,WO888,,W002,WarehouseClerk,WMS,OGR-5566,"{""locationId"":""FG-01"",""lotId"":""L-20251007-01""}"
```

---

## Intent（設計流程）：`e_eventType.csv` 與 `e_triggers.csv`

- `e_eventType.csv`：`name,label_zh_tw,description`
- `e_triggers.csv`：`fromType,toType,guard,guard_zh,notes`  
  - 例：`OperationCheckOut → ReceiveProductionToWarehouse` 搭配 guard `isFinalSegment=true`

> 匯入後，`cypher_load_csv.cypher` 會將 Intent 層的 `TRIGGERS` 用來標記 `:NEXT.cause='triggers'`（若符合），並可在 `cypher_checks.cypher` 比對「應然 vs 實然」。

---

## nodes/ 與 relations/（核心靜態模型）

- **nodes/**：各類節點（`Part / BOM / BOMComponent / ProductSegment / WorkOrder / ...`）  
- **relations/**：各類非事件關係（例如）  
  - `r_part_hasBOM_BOM.csv` → `(Part)-[:HAS_BOM]->(BOM)`  
  - `r_BOM_includesComponent_BOMComponent.csv` → `(BOM)-[:INCLUDES_COMPONENT]->(BOMComponent)`  
  - `r_BOMComponent_consumesChildPart_Part.csv` → `(BOMComponent)-[:CONSUMES_CHILD_PART]->(Part)`  
  - `r_ProductionRouting_hasOrderedSegment_ProductSegment.csv` → `(ProductionRouting)-[:HAS_ORDERED_SEGMENT]->(ProductSegment)`  
  - `r_ProductSegment_realizes_Process.csv` → `(ProductSegment)-[:REALIZES]->(Process)`  
  - ……（其餘依檔名即語意）

> **事件的 performedBy / relatesTo / atSegment / recordedIn / produces / uses** 等關係，皆由 `e_event.csv` 自動物化，不需在 relations/ 另建檔案。

---

## `:NEXT` 的建立邏輯（以 `threadId + occurredAt`）

- 在同一 `threadId` 內，依 `occurredAt` 由小到大串接 `(:Event)-[:NEXT]->(:Event)`。  
- 若 `occurredAt` 完全相同，則以 `eventId` 作為第二排序鍵。  
- `:NEXT.cause` 會在 `(a.eventType)-[:TRIGGERS]->(b.eventType)` 存在時標記為 `'triggers'`，否則 `'unspecified'`。  
- 允許重建：腳本會先清除舊的 `:NEXT` 再產生新的（idempotent）。

---

## 檢核（cypher_checks.cypher）

建議檢核包含（已在檔內備好片段）：
- 每個 `BOMComponent` 是否同時連到 `BOM` 與 `Part`  
- 事件是否都連到 `Thread`、必要對象（如 `WorkOrder`/`ProductSegment`）  
- Intent vs Trace：`TRIGGERS` 的下一步是否有出現，guard 是否被滿足（例如 `isFinalSegment=true`）  
- 同一 thread 內時間是否單調遞增（或回退異常）

---

## 常見問題（FAQ）

- **`attrs` 如何寫？** 作為 **JSON 字串** 放在 CSV；匯入時會合併成事件節點屬性。建議鍵名採下劃線或駝峰，布林值用 `true/false`。  
- **時區？** 建議匯入前轉為 UTC（或包含 `+08:00` 等偏移），確保 `occurredAt` 排序一致。  
- **沒有 ProductSegment 時怎辦？** 留空即可；不會建立 `:AT_SEGMENT`。  
- **`docId` 是否通用？** 目前只針對 `GenerateOutsourcedGoodsReceipt`（PRODUCES）與 `ReceiveOutsourcedToWarehouse`（USES）兩種事件使用；其他事件型別忽略。  
- **要多條 thread 粒度嗎？** 目前以單一 `threadId` 粒度為主；若未來需要多維度，可在模型層新增額外 `Thread` 與 `IN_THREAD{dim}`，本 README 保持單檔不變。

---

## 資料品質建議

- `eventType` 必須出現在 `e_eventType.csv`。  
- `occurredAt` 必須為有效 ISO8601，可被 `datetime()` 解析。  
- `eventId / threadId / workOrderId / productSegmentId / agentId / docId` 等主鍵欄位，請避免前後空白（匯入前做 trim）。  
- `nodes/` 與 `relations/` 的檔頭（header）請勿更名，否則需同步調整 `cypher_load_csv.cypher` 中的對應欄位名。

---

## 版本化與重跑

- 可以安全重跑：`cypher_setup` 使用 `IF NOT EXISTS`，`cypher_load_csv` 使用 `MERGE`，`NEXT` 會先刪再建。  
- 若要全清重建，請先 **備份**，再清空資料庫或刪除既有節點與關係。

---

## 範例查詢

```cypher
// 1) 事件 → 執行者/系統/工單/片段/單據
MATCH (e:Event {eventId:$id})
OPTIONAL MATCH (e)-[:PERFORMED_BY]->(a:Agent)
OPTIONAL MATCH (e)-[:RECORDED_IN]->(sys:System)
OPTIONAL MATCH (e)-[:RELATES_TO]->(w:WorkOrder)
OPTIONAL MATCH (e)-[:AT_SEGMENT]->(s:ProductSegment)
OPTIONAL MATCH (e)-[:PRODUCES]->(ogr:OutsourcedGoodsReceipt)
OPTIONAL MATCH (e)-[:USES]->(ogr2:OutsourcedGoodsReceipt)
RETURN e, a, sys, w, s, ogr, ogr2;

// 2) 用 Intent 預測下一步 & 對照實際
MATCH (t:Thread {threadId:$thread})
MATCH (t)<-[:IN_THREAD]-(last:Event)
WITH t, last ORDER BY last.occurredAt DESC, last.eventId DESC LIMIT 1
MATCH (et:EventType {name:last.eventType})-[:TRIGGERS]->(nextET:EventType)
OPTIONAL MATCH (last)-[:NEXT]->(nxt:Event)
RETURN last.eventType AS current, collect(nextET.name) AS expectedNext, nxt.eventType AS actualNext;
```

---

若你要調整匯入行為（例如把 `attrs` 嚴格驗證、或新增 docId 的其他用途），請修改 `cypher_load_csv.cypher` 對應段落即可

---

```
python3 -m venv venv    
```

```
source venv/bin/activate
```