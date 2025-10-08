# ETL to CSV (DAG + split SQL)

## 檔案結構
```
etl/
  sql/
    n_SalesOrder.sql
    n_SalesOrderItem.sql
    n_WorkOrder.sql   # 需 ids_cte 注入（由 runner 依賴 n_SalesOrderItem 的 CSV）
jobs.yml              # DAG + 參數 + 輸出路徑
nodes/                # 產生的 CSV 會在這裡
etl_runner_dag.py     # 請放在專案根目錄（上一步已提供）
.env                  # REMOTE_DB_URL=...
```

## 執行
```bash
pip install sqlalchemy pymysql pandas pyyaml python-dotenv
export REMOTE_DB_URL="mysql+pymysql://USER:PASS@HOST:3306/DBNAME?charset=utf8mb4"
python etl_runner_dag.py --jobs ./jobs.yml
```

## 說明
- `n_WorkOrder` 依賴 `n_SalesOrderItem`，runner 會先跑上游，然後自動把 `n_SalesOrderItem.csv` 的 `salesOrderItemId`
  注入成 `WITH ids_cte(salesOrderItemId) AS (...)` 分批查詢（`batch_size` 可調）。
- 你可以照樣新增其他 CSV：新增一個 `.sql` 檔，並在 `jobs.yml` 加一個 job（可加 `depends_on` 與 `cte_from_csv`）。
