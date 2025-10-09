SELECT processId,
       processName as name,
       'N' as isOutsourced,
       CreateDatetime as createDatetime,
       UpdateDatetime as updateDatetime
FROM lean.process