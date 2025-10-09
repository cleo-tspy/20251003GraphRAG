-- 說明：
-- - 一筆子件列 b 可能隸屬於不同的 ParentUUID（= 父節點）
-- - 我們把每個 ParentUUID 視為一張 BOM（bomId）
-- - 找出「父節點那一列」p（p.UUID = b.ParentUUID），用 p.PartNo 當 forPartId
SELECT DISTINCT
  p.UUID         AS bomId,     -- BOM 主鍵（父節點 UUID）
  TRIM(p.PartNo) AS forPartId -- 這張 BOM 對應的父件料號（父列 PartNo）
FROM bom b
LEFT JOIN bom p
  ON p.UUID = b.ParentUUID
WHERE b.ParentUUID IS NOT NULL;