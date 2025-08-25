# Streamlit Net GEX (RapidAPI Yahoo Finance 15)

Интерактивный расчёт `Net GEX` по выбранному тикеру и дате экспирации по «упрощённой доллар‑гамме» с калибровкой `k`.

## Развёртывание на streamlit.io (Community Cloud)

1. Сделайте **Fork** этого репозитория на GitHub.
2. В Streamlit Cloud создайте новое приложение, выберите ваш репозиторий и файл `streamlit_app.py`.
3. В разделе **Secrets** добавьте (без кавычек):
```
RAPIDAPI_HOST = yahoo-finance15.p.rapidapi.com
RAPIDAPI_KEY = <ВАШ_КЛЮЧ_RAPIDAPI>
```
> Ключ не храните в репозитории!

## Локальный запуск

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Функционал
- Поле ввода тикера (по умолчанию `SPY`).
- Селектор даты экспирации (автоматически подставляется **ближайшая** доступная).
- Кнопка **«Рассчитать»** — по нажатию запрашиваются сырые данные у провайдера.
- Структурирование данных в таблицу (Strike, Call/Put OI/Volume, IV).
- Расчёт **Net GEX** по методологии: `NetGEX_i = k × (CallOI_i − PutOI_i)`, где `k` калибруется к классической доллар‑гамме.
- Две таблицы: «сырые+IV» и «с Net GEX».
- Отладка: панель **Debug** с метаданными, сырой JSON, лог ошибок, кнопка выгрузки «raw.json».

## Поставщик данных
- RapidAPI host: `yahoo-finance15.p.rapidapi.com`

## Лицензия
MIT
