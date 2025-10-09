SELECT DISTINCT
  b.UUID         AS bomComponentId, -- 線位 UUID
  TRIM(b.PartNo) AS childPartId     -- 子件料號
FROM bom b
WHERE b.UUID   IS NOT NULL
  AND b.PartNo IS NOT NULL;