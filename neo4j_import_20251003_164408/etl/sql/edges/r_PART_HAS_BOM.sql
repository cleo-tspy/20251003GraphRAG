SELECT DISTINCT
  TRIM(p.PartNo) AS partId,   -- 父件料號（forPartId）
  p.UUID         AS bomId     -- 父節點 UUID（BOM 表頭）
FROM bom p
WHERE p.PartNo IS NOT NULL
  AND EXISTS (SELECT 1 FROM bom c WHERE c.ParentUUID = p.UUID);