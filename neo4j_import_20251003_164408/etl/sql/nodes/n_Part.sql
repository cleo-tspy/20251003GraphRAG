SELECT DISTINCT
  TRIM(m.PartNo)                                AS partId,     -- 料號（主鍵）
  NULLIF(TRIM(m.PartName), '')                  AS partName,   -- 品名（空字串→NULL）
  CASE UPPER(TRIM(m.PartType))                                 -- 料件類型：代碼→文字
    WHEN 'P' THEN 'Buy'
    WHEN 'M' THEN 'Make'
    WHEN 'S' THEN 'OutSource'
    WHEN 'X' THEN 'Virtual'
    WHEN ''  THEN NULL
    ELSE TRIM(m.PartType)                        -- 若已是文字或其他代碼，原樣保留
  END                                           AS partType,
  NULLIF(TRIM(m.PartSpec), '')                  AS partSpec,   -- 規格（空字串→NULL）
  COALESCE(NULLIF(TRIM(m.ProduceUnit), ''), 'EA') AS uom       -- 單位，預設 'EA'
FROM material AS m
WHERE m.PartNo IS NOT NULL;