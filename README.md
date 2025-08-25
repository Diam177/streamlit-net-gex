Net GEX — Streamlit app

Как запустить:
1) Добавьте в Streamlit Secrets:
   RAPIDAPI_HOST="yahoo-finance15.p.rapidapi.com"
   RAPIDAPI_KEY="<ваш ключ>"

2) Установите Python 3.10 и requirements.txt.

3) Деплой через streamlit.io, укажите стартовый файл: streamlit_app.py

Debug:
- «Сырые» ответы и снимки расчётов пишутся в /tmp/streamlit_net_gex_debug/
- Логи — в /tmp/streamlit_net_gex_logs/app.log
- В UI есть кнопки для скачивания.
