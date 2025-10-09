SELECT
  b.UUID                                 AS bomComponentId,   -- 線位唯一鍵（對應一條用料關係）
  b.ParentUUID                            AS bomId,            -- 所屬 BOM（連到 n_BOM.csv.bomId）
  TRIM(b.PartNo)                          AS childPartId,      -- 子件料號
  CAST(b.Seq AS UNSIGNED)                 AS componentNo,      -- 線位序號（排序/對位用）
  CAST(b.Quantity AS DECIMAL(18,6))       AS quantity,         -- 做 1 個父件所需子件用量
  COALESCE(NULLIF(TRIM(b.Unit), ''), 'EA') AS uom,              -- 用量單位，預設 'EA'
  NULL                                    AS scrapRate,        -- 先留空；未來需要再補
  NULLIF(TRIM(b.groupCode), '')           AS altGroup,         -- 替代/用料群組碼（若有）
  NULLIF(TRIM(b.Type), '')                AS partType,         -- 子件類型（若有）
  ''                                      AS effectiveFrom,    -- 生效日
  COALESCE(b.ExpireDatetime, b.ValidDatetime) AS effectiveTo,  -- 實際用作「到期/淘汰」日期
  COALESCE(b.Spec, '')                    AS note              -- 附註
FROM bom b
WHERE b.ParentUUID IS NOT NULL
  AND b.PartNo IS NOT NULL;