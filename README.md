# 営業CRM 体験型デモ / Streamlit Cloud向け

## 概要

このアプリは、営業CRMの操作感を体験するためのStreamlit Cloud向けデモです。

## 重要な注意

- 本番データは入力しないでください。
- 実在する個人情報、顧客情報、売上情報、契約情報は入力禁止です。
- 入力内容は一時DBに保存されます。
- Streamlit Cloudの再起動、再デプロイ、リセット操作で入力内容は消える可能性があります。
- GitHub上の `sample_crm_demo.db` はサンプルデータ専用です。

## GitHubに置くファイル

```text
app.py
requirements.txt
sample_crm_demo.db
README.md
.gitignore
```

## Streamlit Cloudでの公開手順

1. GitHubで新規リポジトリを作成
2. このフォルダ内のファイルをアップロード
3. Streamlit Community Cloudへログイン
4. New app を選択
5. GitHubリポジトリを選択
6. Main file path に `app.py` を指定
7. Deploy

## ローカルで動作確認する場合

```bash
pip install -r requirements.txt
streamlit run app.py
```

## デモ機能

- 顧客管理
- 商談管理
- 活動履歴
- 紹介人材管理
- 売上管理
- 原価管理
- 粗利確認
- CSV出力
- サンプルデータへのリセット
