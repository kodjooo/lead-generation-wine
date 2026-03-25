Документация (v3): система поиска сайтов торговых центров и агентств недвижимости по списку городов через Yandex Search API (ночные отложенные запросы), дедупликация, сбор email и персонализированная рассылка

Дата: 2025‑10‑17
Автор: Марк (проект: AI‑аутрич/лидогенерация)

Актуализация от 2026‑03‑20
──────────────────────────────────────────────────────────────────────────────
Текущая версия проекта больше не строится вокруг ниш. Входом является Google Sheets со списком городов и служебными флагами поиска.

Целевой pipeline:
• Google Sheets (`city`, `country`, `batch_tag`, `search_malls`, `search_agencies`)
• генерация стабильных Yandex Search API deferred-запросов по каждому городу
• фильтрация результатов с отсечением агрегаторов, каталогов и нерелевантных сайтов
• сохранение только уникальных сайтов двух типов: `mall` и `real_estate_agency`
• обход сайтов и сбор email
• генерация уникальных писем по двум веткам
• отправка через MX-маршрутизацию

Что сохраняется:
• ночные deferred-запросы Yandex Search API
• IAM-авторизация Yandex Cloud
• дедупликация по домену
• обход сайтов, генерация писем и MX-routing

Что убрано из основной бизнес-логики:
• генерация запросов по нишам
• зависимость от `niche` как основной сущности ввода

──────────────────────────────────────────────────────────────────────────────

1) Цель и ключевые принципы
──────────────────────────────────────────────────────────────────────────────
Задача — автономно находить ~5000 уникальных компаний в месяц по тематике/региону,
извлекать контакты, генерировать «человечные» письма с понятной бизнес‑пользой
и безопасно отправлять их, сохраняя журнал и статусы прохождения по пайплайну.

Ключевые принципы:
• Только ночные отложенные (deferred) запросы Yandex Search API — минимальная цена.
• Полный отказ от HTML‑парсинга Яндекса (никаких прокси/капч): только официальный API.
• Жесткая дедупликация на уровне домена/компании до этапа контактов и рассылки.
• Отслеживание статусов по всем сущностям (компания, контакт, письмо, SERP‑запрос).
• Идемпотентность: любой шаг можно перезапустить без «двоения» записей.
• Политика уважения Opt‑out и юридическое соответствие (GDPR/152‑ФЗ и пр.).

Ожидаемый объём:
• Ночных deferred‑запросов в месяц: ориентир 500‑1000 (зависит от “результатов на запрос”).
• Уникальных компаний после дедупликации: ~5000/мес.
• Рассылка: стартово до 100 писем/день (Gmail SMTP), масштабируемо через Mailgun/Brevo.


2) Архитектура и папки проекта
──────────────────────────────────────────────────────────────────────────────
/project-root
│
├── main.py                     # оркестратор пайплайна (шаги + планировщик)
├── config.py                   # конфиги (ключи, лимиты, окна, модели, тайминги)
├── .env                        # секреты (YC tokens, ROUTING_*, GMAIL_*/YANDEX_*, OpenAI), не коммитим
│
├── modules/
│   ├── yandex_deferred.py      # создание и опрос deferred‑запросов к Yandex Search API
│   ├── serp_ingest.py          # разбор результатов, нормализация URL/хостов
│   ├── deduplicate.py          # дедупликация компаний/доменов (чёткие и «фаззи» правила)
│   ├── enrich_contacts.py      # загрузка сайта, нахождение «Контактов», извлечение email/телефонов
│   ├── generate_email_gpt.py   # генерация темы/текста письма (человечный тон + выгоды)
│   ├── send_email.py           # отправка писем (Gmail SMTP / Mailgun), трекинг статусов
│   ├── utils/
│   │   ├── db.py               # слой БД (SQLite/PostgreSQL), миграции, транзакции
│   │   ├── logger.py           # структурированное логирование (JSON)
│   │   ├── rate_limit.py       # все задержки/бэкоффы/окна (ночные/дневные)
│   │   ├── normalize.py        # каноникализация URL/доменов, punycode, сравнение имен
│   │   └── email_guard.py      # фильтры/«не спам» правила, unsubscribe, отработка жалоб
│
├── data/                       # артефакты импорта/экспорта (по желанию)
│   ├── exports/companies.csv
│   ├── exports/contacts.csv
│   └── exports/sent.csv
│
└── migrations/                 # DDL для БД (если не ORM‑миграции)


3) Модель данных и статусы
──────────────────────────────────────────────────────────────────────────────

3.1 Таблицы (минимально необходимое)
-------------------------------------
serp_queries
- id (pk)
- query_text               # текст поискового запроса (с тематиками/гео/языком)
- region_code              # регион Яндекса, если используем
- lang                     # язык интерфейса/результатов
- request_mode             # 'deferred'
- submitted_at             # когда отправили отложенный запрос
- operation_id             # id операции в Yandex API
- status                   # QUEUED | SUBMITTED | READY | FAILED | EXPIRED
- results_total            # сколько результатов вернул API (после fetch)
- window_tag               # NIGHT_UTC_WINDOW (для аудита)
- cost_estimate            # оценка стоимости (для биллинга/аналитики)
- retry_count              # число повторов при сбое
- error_payload            # последняя ошибка/трассировка

serp_results
- id (pk)
- serp_query_id (fk -> serp_queries.id)
- position                 # позиция в выдаче
- url_raw                  # URL из результата (как пришёл)
- title                    # заголовок
- snippet                  # сниппет
- host_raw                 # хост из URL до нормализации
- host_norm                # нормализованный хост (normalize_host())
- canonical_url            # канонический URL (после HEAD/redirect‑каноникализации)
- company_name_guess       # эвристически извлечённое имя компании (если доступно)
- is_processed             # флаг: разобран/нет
- dedup_status             # NEW | DUP_HOST | DUP_CANONICAL | DUP_FUZZY | UNIQUE
- dedup_key                # ключ уникальности (host_norm или доменное семейство)

companies
- id (pk)
- canonical_domain         # основной домен компании (host_norm)
- company_name             # финальное имя (после объединения/энричмента)
- website_url              # предпочитаемая главная страница https://<domain>/
- country                  # по возможности (из сайта/Whois/контента)
- lang                     # основной язык сайта
- source                   # 'yandex_search_api'
- lifecycle_status         # NEW | ENRICHING | READY_TO_CONTACT | CONTACTED |
                           # REPLIED | DISQUALIFIED | OPTOUT | QUALIFIED
- first_seen_at
- last_seen_at

contacts
- id (pk)
- company_id (fk -> companies.id)
- email
- phone
- source_url               # страница, где найден контакт
- confidence               # 0..1 (эвристика валидности/релевантности)
- created_at

emails_outbox
- id (pk)
- company_id (fk)
- to_email
- subject
- body_text
- generation_model         # 'gpt-4o-mini'
- generated_at
- send_status              # PENDING | SENDING | SENT | BOUNCED | REPLIED | UNSUB
- smtp_provider            # 'gmail' | 'mailgun' | ...
- smtp_response            # последние коды/ответ
- message_id               # id письма у провайдера
- followup_of              # id первого письма (для цепочек)
- updated_at

audit_log (опционально)
- id, entity_type, entity_id, action, payload, at


3.2 Ключевые статусы и переходы
-------------------------------
SERP запрос (serp_queries.status):
QUEUED → SUBMITTED → READY | FAILED | EXPIRED

SERP результат (serp_results.dedup_status):
NEW → (после нормализации/сверки) → UNIQUE | DUP_HOST | DUP_CANONICAL | DUP_FUZZY

Компания (companies.lifecycle_status):
NEW → ENRICHING → READY_TO_CONTACT → CONTACTED → (REPLIED | DISQUALIFIED | OPTOUT | QUALIFIED)

Письмо (emails_outbox.send_status):
PENDING → SENDING → SENT → (REPLIED | BOUNCED | UNSUB)


4) Ночные deferred‑запросы Yandex Search API
──────────────────────────────────────────────────────────────────────────────
• Почему: отложенные запросы в «ночное» окно у Яндекса кратно дешевле синхронных.
• Как: создаём операции (operation_id), периодически опрашиваем их готовность,
  затем забираем результаты пачками и фиксируем в serp_results.

Параметры/настройки (config.py):
- YC_FOLDER_ID / YC_IAM_TOKEN / (или сервисный аккаунт + ключ) — для авторизации.
- YANDEX_SEARCH_ENDPOINTS = {
    "create": "<URL-создания-операции>",
    "get_operation": "<URL-проверки-готовности>",
    "get_results": "<URL-загрузки-результатов>"
  }
- NIGHT_UTC_WINDOW = ("20:00", "05:59")     # Пример. Подбирается под политику Яндекс API.
- DEFERRED_BATCH = 200                      # Сколько операций создаём в «ночном окне» за тик
- POLL_INTERVAL_SEC = 60                    # как часто спрашиваем статус операции
- RESULTS_PER_REQUEST = 50                  # целевой объём результатов на запрос (если доступно)
- MAX_RETRIES = 3                           # повторы при сетевых/API сбоях

Псевдокод отправки (create):
for q in queries_to_run:
  if now in NIGHT_UTC_WINDOW:
      op_id = yandex_deferred.create(query=q, region=..., lang=..., num=RESULTS_PER_REQUEST)
      save serp_queries(row: SUBMITTED, operation_id=op_id)
  else:
      queue serp_queries(row: QUEUED)

Псевдокод опроса (poll):
for op in serp_queries where status=SUBMITTED:
  s = yandex_deferred.get_operation(op.operation_id)
  if s.done:
      r = yandex_deferred.get_results(op.operation_id)
      save serp_results(r.items)
      mark serp_queries.status = READY
  elif s.error:
      mark serp_queries.status = FAILED (+payload)
  else:
      sleep(POLL_INTERVAL_SEC)

Примечания:
• Количество результатов на запрос регулируется параметром API (если доступно).
• На стороне ingest фиксируем position/url/title/snippet/host, чтобы не потерять контекст.
• Ведём учёт стоимости: deferred‑запросы ночные — считали как ~0.21$ за 1000, дневные ~0.25$ (оценка).


5) Нормализация и дедупликация
──────────────────────────────────────────────────────────────────────────────
Цель — исключить повторы до захода на сайты и генерации писем.

5.1 Нормализация URL/хоста
• normalize_host(host):
  - приведение к нижнему регистру
  - удаление 'www.'
  - punycode → Unicode (или наоборот, унификация)
  - обрезка портов/хвостов, /index.*
  - стандартные TLD корректности
• canonicalize_url(url):
  - HEAD/GET с follow‑redirects → зафиксировать итоговый canonical
  - если видим meta canonical — учитываем

5.2 Ключи дедупликации
• K1: host_norm (жёсткий ключ по домену)
• K2: canonical_url (если есть)
• K3: fuzzy bundle name (фаззи‑сверка названий компаний)
  - normalize_company_name(): lower, удалить «ООО/ЗАО/ИП/LLC/Inc», убрать пунктуацию/общие слова
  - сравнение Levenshtein/Jaro‑Winkler ≥ threshold (например, 0.9)
• K4: семейство доменов по email‑доменам (если один сайт не единственный, а сеть)

Правила:
- Если найден host_norm уже в companies → serp_results.dedup_status = DUP_HOST
- Если canonical_url уже известен → DUP_CANONICAL
- Если name_fuzzy похож на существующую company_name → DUP_FUZZY
- Иначе → UNIQUE → создаём запись в companies (status=NEW)

Уникальные ограничения БД:
- UNIQUE(canonical_domain)
- INDEX по host_norm, canonical_url
- Вторичный индекс по normalized company_name для фаззи‑поиска (в коде, не в БД)


6) Обогащение контактов (ENRICHING)
──────────────────────────────────────────────────────────────────────────────
Для companies со статусом NEW → переходим к ENRICHING:
• Загружаем главную страницу + пытаемся определить страницы «Контакты»/«О нас»:
  - эвристики по ссылкам: /contacts, /kontakty, /contact, /kontakt, /svyaz, /about
  - карта сайта (/sitemap.xml) при наличии, robots.txt — учитывать crawl-delay
• Извлекаем email/телефоны (регексы + валидация):
  - email: RFC‑5322, запрет на обрывки (например, user(at)domain — нормализуем)
  - телефон: +код страны, длины, фильтр «мусора»
• Валидность/релевантность:
  - по ключевым словам рядом (sales, info, hr — можно снижать приоритет)
  - рейтинг confidence 0..1 (наша эвристика + доменный приоритет)
• Сохраняем в contacts до 3 email и 2‑3 телефонов с source_url и confidence.
• Если ничего не нашли — DISQUALIFIED (или оставить READY_TO_CONTACT без email, только телефон).

Рекомендации по скорости:
• Без прокси, но с паузами 30‑45 сек между доменами, таймауты 10‑15 сек, экспоненциальный бэкофф 60s→5m при 403/429.
• Кешировать уже посещённые URL (не долбить повторно).


7) Генерация писем (человечный тон + выгоды)
──────────────────────────────────────────────────────────────────────────────
Для companies со статусом READY_TO_CONTACT:
• Вход в LLM: компания (имя, описание), направление, регион, чем занимается.
• Выход LLM (JSON): {subject, body} — без рекламы; 6‑8 предложений; 2‑3 идеи автоматизации,
  и обязательно «что это даст» (меньше ручной рутины, экономия времени/денег, меньше ошибок).
• В конец: фраза про кейсы («…см. прикреплённый документ/ссылку»).
• Модель: gpt‑4o‑mini (экономичный режим), детерминированность ~0.7, разнообразие тем.

Идемпотентность:
• emails_outbox уникален по (company_id, to_email, subject hash) — не дублировать.
• Повторы генерации → перезапись черновика, если не «SENT».


8) Отправка писем и антиспам‑гигиена
──────────────────────────────────────────────────────────────────────────────
Стартовые провайдеры: Gmail (до ~100/день) + резерв Яндекс для MX класса RU.
• Случайные паузы 2‑7 мин между письмами (на акк).
• DKIM/SPF/DMARC — настроить на домене, если используем кастомный домен отправителя.
• Unsubscribe/Opt‑out: отдельный трекер ссылок или простое «ответьте STOP».

Статусы:
• PENDING → SENDING → SENT / BOUNCED / REPLIED / UNSUB
• Любой ответ «не интересует/удалите меня» → компания.lifecycle_status = OPTOUT, в emails_guard заносим домен/email к запрету.

Масштабирование:
• Переезд на Mailgun/Brevo: параллельно 2‑3 домена/поддомена, раздельные IP (тепление IP, sender reputation).


9) Планировщик и окна
──────────────────────────────────────────────────────────────────────────────
• Ночные окна (UTC) для создания deferred‑операций Яндекса: NIGHT_UTC_WINDOW в config.
• Дневные часы — агрегируем, дедуплицируем, обогащаем, генерируем письма, отправляем.
• Параллелизм:
  - Создание операций: пачками DEFERRED_BATCH каждые 5‑10 минут в окне.
  - Опрос операций: POLL_INTERVAL_SEC, ограничить одновременные запросы к API.
  - Обогащение сайтов: в 1‑3 потока, чтобы не ловить 403/429.
  - Отправка писем: очередью, не более X/час на провайдера.


10) Конфигурация (config.py) — ключи и лимиты
──────────────────────────────────────────────────────────────────────────────
YANDEX:
  YC_FOLDER_ID="..."
  YC_IAM_TOKEN="..."               # обновление токена — отдельный раннер/скрипт
  NIGHT_UTC_WINDOW=("20:00","05:59")
  RESULTS_PER_REQUEST=50
  DEFERRED_BATCH=200
  POLL_INTERVAL_SEC=60
  MAX_RETRIES=3

OPENAI:
  MODEL="gpt-4o-mini"
  TEMPERATURE=0.7
  CASES_URL="https://docs.google.com/document/d/..."
  MAX_BODY_CHARS=1200

ROUTING:
  ENABLED=true
  MX_CACHE_TTL_HOURS=168
  DNS_TIMEOUT_MS=1500
  DNS_RESOLVERS=["1.1.1.1","8.8.8.8"]
  RU_PATTERNS=[
    "1c.ru","aeroflot.ru","alfabank.ru","beeline.ru","beget.com","facct.email","facct.ru",
    "gazprom.ru","gosuslugi.ru","hh.ru","kommersant.ru","lancloud.ru","lukoil.com","magnit.ru",
    "mail.ru","masterhost.ru","mchost.ru","megafon.ru","mos.ru","mts.ru","netangels.ru","nornik.ru",
    "novatek.ru","pochta.ru","proactivity.ru","rambler-co.ru","rambler.ru","rbc.ru","rosatom.ru",
    "roscosmos.ru","rt.ru","runity.ru","russianpost.ru","sber.ru","sberbank.ru","selectel.org",
    "sevstar.net","sovcombank.ru","sprinthost.ru","tatneft.ru","tbank.ru","timeweb.ru","vtb.ru",
    "vtbcapital.ru","wildberries.ru","x5.ru","yandex.net","yandex.ru"
  ]
  RU_TLDS=[".ru",".su",".xn--p1ai",".xn--p1acf",".moscow",".moskva",".xn--80adxhks"]
  FORCE_RU_DOMAINS=["yandex.ru","mail.ru","bk.ru","inbox.ru","list.ru","rambler.ru"]

GMAIL:
  GMAIL_SMTP_HOST="smtp.gmail.com"
  GMAIL_SMTP_PORT=587
  TLS=true
  USER="you@example.com"
  APP_PASSWORD="..."
  FROM="Имя Отправителя <you@example.com>"
  DAILY_LIMIT=100
  SLEEP_BETWEEN=(120,420)

YANDEX:
  YANDEX_SMTP_HOST="smtp.yandex.ru"
  YANDEX_SMTP_PORT=465
  SSL=true
  USER="mark@yandex.ru"
  APP_PASSWORD="..."
  FROM="Имя Отправителя <mark@yandex.ru>"

CRAWL:
  FETCH_TIMEOUT=15
  SLEEP_BETWEEN_SITES=(30,45)
  RETRY_BACKOFF=[60, 300, 1800]    # 1мин, 5мин, 30мин
  MAX_EMAILS_PER_COMPANY=3
  MAX_PHONES_PER_COMPANY=3


11) Потоки (ETL) и оркестрация (main.py)
──────────────────────────────────────────────────────────────────────────────
Шаг 1. Планирование запросов
  • Формируем пул query_text (тема × регионы × язык).
  • Создаём записи serp_queries со status=QUEUED.

Шаг 2. Создание deferred операций (в ночное окно)
  • Берём QUEUED, создаём операции в Яндекс → SUBMITTED, сохраняем operation_id.

Шаг 3. Опрос и загрузка результатов
  • Для SUBMITTED — poll до READY. Готовые → грузим результаты в serp_results.

Шаг 4. Нормализация и дедупликация
  • serp_ingest → normalize_host/canonical_url → deduplicate →
    UNIQUE → companies.NEW; DUP_* → помечаем и не пропускаем дальше.

Шаг 5. Обогащение компаний (ENRICHING)
  • enrich_contacts → находим emails/phones →
    если контакты найдены → companies.READY_TO_CONTACT
    иначе → DISQUALIFIED (или Ready без email, по стратегии).

Шаг 6. Генерация писем
  • generate_email_gpt → emails_outbox (PENDING).

Шаг 7. Отправка писем
  • send_email → SENDING → SENT/BOUNCED; обновляем lifecycle и outbox статус.

Шаг 8. Обработка ответов/отписок
  • webhook/IMAP‑парсер (по возможности) → REPLIED/UNSUB → lifecycle и guard.


12) Идемпотентность, транзакции, восстановления
──────────────────────────────────────────────────────────────────────────────
• Все записи получают стабильные id/ключи уникальности (host_norm, canonical_domain).
• Повторный прогон любого шага не должен создавать дублей (UNIQUE‑констрейны + UPSERT).
• Транзакции вокруг критических секций (создание компании, запись контактов).
• Резервные копии БД раз в N часов/дней, write‑ahead logging (если SQLite — включить WAL).
• Ретрай‑таблицы для упавших задач (FAILED/EXPIRED с причинами).


13) Качество, тестирование, мониторинг
──────────────────────────────────────────────────────────────────────────────
Юнит‑тесты:
• normalize_host/canonicalize_url (краевые кейсы, IDN, редиректы)
• дедуп‑правила (host/canonical/fuzzy)
• email/phone regex + валидатор

Интеграционные тесты:
• мок Yandex API (deferred create/poll/results)
• end‑to‑end на 2‑3 тестовых запросах: → UNIQUE  → ENRICH → EMAIL

Мониторинг:
• Дэшборд (Grafana/Metabase) по статусам: сколько QUEUED/SUBMITTED/READY,
  доля DUP_*, сколько NEW→READY_TO_CONTACT, CTR рассылки, bounce rate.
• Алерты: много FAILED у Yandex / рост 403/429 при крауле / рост BOUNCED.


14) Безопасность и правовые аспекты
──────────────────────────────────────────────────────────────────────────────
• Хранить только бизнес‑контакты (публичные корпоративные email/телефоны).
• Обязательный Opt‑out: чёткий канал отписки, мгновенное внесение в stop‑лист.
• Соблюдение robots.txt и crawl‑delay при крауле сайтов.
• Секреты в .env, ротация токенов, ограничение прав сервисных аккаунтов.
• DMARC/SPF/DKIM на доменах отправителя, честные From/Reply‑To.


15) Производительность и оценки стоимости
──────────────────────────────────────────────────────────────────────────────
• Deferred Yandex (ночные): ориентир 500‑1000 запросов/мес → <$1 за API‑запросы.
• Генерация писем (gpt‑4o‑mini): ~$1 за 1000 писем (зависит от токенов).
• Gmail SMTP: $0 (лимит около 100 писем/день/акк).
• Итого по API‑стоимости — символическая величина при твоём объёме.

Скорость:
• Ночных create/poll легко масштабируются (операций в час много).
• Узкие места: обогащение сайтов (деликатно), отправка писем (лимиты провайдеров).


16) Чек‑лист внедрения
──────────────────────────────────────────────────────────────────────────────
[ ] Завести Yandex Cloud проект, получить IAM‑токен/аккаунт, настроить доступ к Search API.
[ ] Заполнить .env (YC_*, ROUTING_*, GMAIL_*, YANDEX_*, OpenAI_*).
[ ] Прописать NIGHT_UTC_WINDOW и расписание (cron/systemd/Docker).
[ ] Прогнать миграции БД, включить WAL (если SQLite).
[ ] Задать темы/регионы/языки запросов (генерация query_text).
[ ] Тест 5‑10 запросов overnight → проверить serp_results и дедуп.
[ ] Прогнать ENRICHING на 20‑30 доменах → оценить контакт‑yield.
[ ] Проверить генерацию писем и антиспам‑гигиену.
[ ] Включить рассылку на узком скоупе (10‑20 писем/день), затем расширять.
[ ] Встроить алерты и бэкапы.

Конец документа.

17) Методы Yandex Search API (REST, v2) — отложенный режим (DEFERRED)
──────────────────────────────────────────────────────────────────────────────
Назначение: запуск ночных «отложенных» поисковых запросов, опрос статуса и получение результата (XML/HTML) для дальнейшей нормализации, дедупликации и извлечения доменов/сайтов компаний.

17.1 Аутентификация и доступ
• Авторизация: заголовок `Authorization: Bearer <IAM_TOKEN>` (короткоживущий IAM‑токен).
• Права: роли сервисному аккаунту — `search-api.webSearch.user`.
• Идентификатор каталога (папки): `folder_id` в теле запроса (обязателен).
• Альтернатива IAM: API‑ключ сервисного аккаунта (меняется формат заголовка, но для пайплайна рекомендуем IAM).

17.2 Создание отложенного запроса (CREATE)
HTTP
  POST https://searchapi.api.cloud.yandex.net/v2/web/searchAsync
Headers
  Authorization: Bearer <IAM_TOKEN>
  Content-Type: application/json
Body (минимальный состав; используем snake_case поля API v2):
{
  "query": {
    "search_type": "SEARCH_TYPE_RU",         // RU/TR/COM/KK/BE/UZ
    "query_text": "<строка запроса до 400 символов>",
    "family_mode": "FAMILY_MODE_MODERATE",   // по умолчанию
    "page": 0,                               // 0 — первая страница
    "fix_typo_mode": "FIX_TYPO_MODE_ON"      // или OFF — для строгого режима
  },
  "sort_spec": {
    "sort_mode": "SORT_MODE_BY_RELEVANCE",   // либо BY_TIME
    "sort_order": "SORT_ORDER_DESC"          // DESC | ASC
  },
  "group_spec": {
    "group_mode": "GROUP_MODE_DEEP",         // ВАЖНО: группировка по домену
    "groups_on_page": 50,                    // XML: 1..100; HTML: 5..50
    "docs_in_group": 1                       // 1..3; для уникальных доменов ставим 1
  },
  "max_passages": 3,                         // 1..5 (влияет на сниппет)
  "region": 225,                             // lr: 225=Россия; 187=Украина; 149=Беларусь; 159=Казахстан; и т.д.
  "l10n": "LOCALIZATION_RU",                 // язык уведомлений/подписей
  "folder_id": "<ID каталога в YC>",
  "response_format": "FORMAT_XML",           // FORMAT_XML | FORMAT_HTML
  "user_agent": "Mozilla/5.0 ..."            // опционально (для мобильной выдачи укажи мобильный UA)
}

Ответ (успешный старт операции):
{
  "done": false,
  "id": "<operation_id>",
  "description": "WEB search async",
  "createdAt": "...",
  "modifiedAt": "..."
}

Заметки по выбору параметров для нашей задачи (уникальные домены):
• group_mode=GROUP_MODE_DEEP и docs_in_group=1 — минимизирует повторы со страниц одного домена.
• response_format=FORMAT_XML — проще парсить (структурированные теги url/domain/title/passages).
• groups_on_page: XML позволяет до 100; в текущем пайплайне используем 50, чтобы не раздувать хвост выдачи и ускорить homepage-check.
• region/lr — ставим нужную страну/город: 225 (RU), 187 (UA), 149 (BY), 159 (KZ) и др.
• fix_typo_mode=OFF — если нужен строгий матч; ON — если допускаем автокоррекцию.

17.3 Проверка статуса операции (GET OPERATION)
HTTP
  GET https://operation.api.cloud.yandex.net/operations/<operation_id>
Headers
  Authorization: Bearer <IAM_TOKEN>

Фрагмент успешного ответа (когда готово):
{
  "done": true,
  "response": {
    "@type": "type.googleapis.com/yandex.cloud.searchapi.v2.WebSearchResponse",
    "rawData": "<Base64_encoded_XML_or_HTML>"
  },
  "id": "<operation_id>",
  "description": "WEB search async",
  "createdAt": "...",
  "modifiedAt": "..."
}

• Если done=false — повторить опрос через интервал POLL_INTERVAL_SEC.
• Минимальное время обработки deferred‑запроса — ~5 минут (может быть дольше в пике).

17.4 Получение результата и декодирование
В deferred режиме сам результат (XML/HTML) возвращается Base64‑строкой в поле `response.rawData` объекта Operation.

• Шаги:
  1) GET /operations/<operation_id> → сохранить JSON.
  2) Извлечь строку `.response.rawData`.
  3) Base64‑decode → `result.xml` (или `result.html`).

XML удобно парсить по структуре:
- В каждом `<group>` содержится один или несколько `<doc>`, но при docs_in_group=1 — один документ на домен.
- Полезные теги: `<url>`, `<domain>`, `<title>`, `<headline>`, `<passages> ... </passages>`.
- Навигация по страницам задаётся `page` и параметрами `groupby` (groups-on-page, docs-in-group).

17.5 Квоты и лимиты (важное для планировщика)
• Deferred запросов в час: до ~35 000.
• Deferred запросов в секунду: до 10 rps.
• Запросов/сек на получение результатов deferred: до 10 rps.
• Лимит результатов: до 250 (на сущность «результат», зависящий от группировок).
• Макс. длина запроса: 400 символов; макс. слов — 40.
• Минимальное время обработки deferred: ~5 минут.
(Фактические квоты/лимиты уточняются в панели и документации; держим параметры конфигурации гибкими.)

17.6 Тарифы (для ночных deferred)
• Дневные deferred: ~$0.250000 за 1000 запросов.
• Ночные deferred: ~$0.208333 за 1000 запросов.
(Биллинг привязан к региону обслуживания и юр. лицу договора; НДС учитывается отдельно.)

17.7 Рекомендованные пресеты для нашей системы
• Поисковый тип: `SEARCH_TYPE_RU` (если работаем с РУ‑сегментом); для интернациональных — `SEARCH_TYPE_COM`.
• Группировка: `GROUP_MODE_DEEP`, `docs_in_group=1`, `groups_on_page=50` (XML).
• Формат ответа: `FORMAT_XML`.
• Регион: выбираем `region` (lr) по целевой стране/городу; по умолчанию 225 (Россия).
• Язык локализации: `LOCALIZATION_RU` (или нужный).
• Частота опроса операции: 60 сек; таймаут ожидания — до нескольких часов (с backoff).
• Ночные окна: 00:00–07:59 (UTC+3) — под цену «ночных deferred».
• Идемпотентность: на уровне `serp_queries` хранить body‑hash запроса; не создавать дубликаты операций.
• Прием результатов: декодировать Base64 → XML → сохранять в `serp_results` с position/url/domain/title/snippet.

17.8 Обработка ошибок
• HTTP 4xx/5xx — фиксировать payload, делать ограниченное число ретраев (MAX_RETRIES).
• Ошибки Operation (`error` в объекте) — переводить запрос в FAILED с сохранением причины.
• Превышение квот — увеличивать паузы, переносить создание операций в следующее ночное окно.
• Пустые/неинформативные ответы — логировать для улучшения генерации запросов.
