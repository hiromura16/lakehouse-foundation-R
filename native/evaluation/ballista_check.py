"""Schedulerに投入した集計をExecutor経由で読み出す。"""

from ballista import BallistaSessionContext

context = BallistaSessionContext("df://ballista-scheduler:50050")
frame = context.sql("SELECT SUM(n) AS total FROM (VALUES (2), (3), (5)) t(n)")
if frame.to_pylist() != [{"total": 10}]:
    raise ValueError("分散SQLの結果が期待値10と一致しません。")
print("Ballistaの分散SQLで合計10を確認しました。")
