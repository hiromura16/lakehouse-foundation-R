# 合成入力を最初に作る

<!-- doc-purpose:start -->
> **役割:** 実データを使わずに最初の6契約素材を生成し、出力と後続処理の境界を確認する。
>
> **対象読者:** 初めて使う人 / データ利用担当
>
> **読むタイミング:** 空の検証用landingに小さい固定条件の合成入力を初めて用意するとき。
>
> **前提となる資料:** [Foundation Rの初回導入と操作](<../docs/native/START.md>)
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<../docs/native/ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../docs/DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

Foundation Rの試験では、[初回導入](../docs/native/START.md) のコマンドで固定条件の合成データを生成します。出力はcheckout外の専用landingに置き、取込と変換はワーカーに明示的に依頼します。

まず日次生産と受注・出荷を確認します。設備データや異常系も試す場合は、[受入手順と合格条件](../docs/native/ACCEPTANCE.md) を参照してください。外部アプリからファイルを受け取る条件は [入力契約](../docs/native/SOURCES.md) にあります。
