SELECT DISTINCT
  b.ParentUUID AS bomId,         -- 對應 n_BOM.csv 的 bomId（父節點 UUID）
  b.UUID       AS bomComponentId  -- 對應 n_BOMComponent.csv 的 bomComponentId（線位 UUID）
FROM bom b
WHERE b.ParentUUID IS NOT NULL
  AND b.UUID IS NOT NULL;