# Lead Generation Pipeline

Проект автоматизирует поиск сайтов торговых центров и агентств недвижимости через Yandex Search API, сбор email и персонализированную рассылку писем. Вся инфраструктура ориентирована на работу в Docker.

## Структура проекта

- `app/` — исходный код служб (`main`, `scheduler`, `worker`).
- `docs/` — требования, архитектура, план внедрения.
- `Dockerfile` — базовый образ Python 3.12.
- `docker-compose.yml` — оркестрация сервисов (`app`, `scheduler`, `worker`, `db`, `redis`).
- `.env`, `.env.example` — переменные окружения (секреты не коммитим, `.env` добавлен в `.gitignore`).

## Требования

- Docker 24.0+ и docker compose plugin
- Возможность открыть исходящие соединения на `smtp.gmail.com:587`
- Доступ к Yandex Search API и Google Sheets
- Для Playwright fallback в Docker браузер Chromium устанавливается автоматически на этапе сборки образа

## Подготовка окружения

```bash
cp .env.example .env  # заполните значения согласно комментариям
docker compose pull   # заранее загрузить базовые образы
```

После заполнения `.env` примените миграции (см. раздел «Миграции БД») и запустите compose.

## Быстрый старт в Docker Compose

```bash
docker compose up --build
```

Сервисы:
- `app` — оркестратор полного цикла (deferred → дедуп → enrichment → рассылка).
- `scheduler` — постановка deferred-запросов и polling операций.
- `worker` — enrichment контактов и отправка писем.
- `db` — PostgreSQL 16 (storage для пайплайна).
- `redis` — брокер задач/кэш.

> Redis по умолчанию доступен только внутри сети docker compose. Это позволяет запускать стек на серверах, где уже установлен системный Redis (нет конфликта портов на 6379). Если нужен доступ с хоста, создайте `docker-compose.override.yml` и добавьте в нём `ports` для сервиса `redis`, например:

```yaml
services:
  redis:
    ports:
      - "6379:6379"
```

## Как работает пайплайн

1. **Подготовка данных.** Города заносятся в таблицу Google Sheets (например, лист `CITIES_INPUT`) со столбцами `city`, `country`, `batch_tag`, `search_malls`, `search_agencies`. Сервис `SheetSyncService` превращает каждую строку в набор стабильных поисковых запросов через `QueryGenerator`: для каждого города формируются отдельные deferred-запросы для сайтов ТЦ и агентств недвижимости, выбирается регион (`lr`), рассчитывается ночное окно и время запуска, а в `serp_queries.metadata` сохраняется тип сущности.
2. **Планирование и Yandex Search.** Контейнер `scheduler` берёт pending-запросы, проверяет ночное окно и квоты, затем через `YandexDeferredClient` создаёт deferred-операции (таблица `serp_operations`). Клиент автоматически обновляет IAM токен, следит за rate-limit и при необходимости откладывает выполнение. В течение ночи `scheduler` и `app` опрашивают операции (`get_operation`), пока не получат Base64-выдачу.
3. **Парсинг SERP.** Когда операция завершена, `SerpIngestService` декодирует XML, нормализует URL/домены, фильтрует запрещённые домены, отбрасывает URL-паттерны агрегаторов и затем подтверждает кандидата по контенту главной страницы. Для спорных кандидатов после homepage-проверки может вызываться OpenAI fallback, который одним verdict уточняет тип сайта (`official_mall_site`, `mall_tenant_site`, `official_real_estate_agency_site`, `developer_site` и т.д.) и фактический город. В БД сохраняются только релевантные домены; тип сущности сохраняется в `companies.industry`, а город-источник в `companies.region`.
4. **Дедупликация компаний.** `DeduplicationService` устраняет повторы на уровне домена и исключает дубликаты из дальнейшего пайплайна.
5. **Обогащение контактов.** Воркер `worker` и оркестратор выбирают компании без контактов. `ContactEnricher` строит список страниц (`/`, `/contact`, `/kontakty`, `/arenda`, `/leasing`, `/team`, `/offices` и др.), использует браузерные заголовки, небольшие паузы между запросами и retry/backoff для базового обхода anti-bot защиты, затем сохраняет фрагмент главной страницы в `companies.attributes.homepage_excerpt` и извлекает email как из `mailto:`, так и из текстового контента страницы. Если статический HTML не дал контактов и включён `CONTACT_ENRICH_PLAYWRIGHT_ENABLED`, сервис повторяет обход через headless Chromium и анализирует уже полностью отрендеренный DOM.
6. **Генерация писем.** Для каждого email без рассылки оркестратор собирает `CompanyBrief` и `OfferBrief`, затем `EmailGenerator` генерирует разные письма для `mall` и `real_estate_agency`. При отсутствии ключа OpenAI используется fallback-шаблон.
8. **Доставка.** Во время рабочего окна сервис выбирает `scheduled` записи с просроченным `scheduled_for`, повторно проверяет opt-out и валидирует email (пустые строки и номера телефонов помечаются `invalid_email` и не попадают к SMTP). Для валидных адресов выбирается канал (Gmail или Яндекс), выполняется отправка и фиксируются `sent_at`, `message_id`, `metadata.route`. Повторы исключены: отбор идёт с блокировкой строк (SKIP LOCKED).
9. **Статусы компаний.** После обхода контактов компания получает `contacts_ready`. Если email не найден, записывается `contacts_not_found`, и оркестратор её больше не обрабатывает.

## Локальный запуск

1. **Подготовьте окружение:**
   ```bash
   cp .env.example .env
   # Заполните .env значениями для Yandex, Google, SMTP, OPENAI
   docker compose pull
   ```
2. **Примените миграции:**
   ```bash
   docker compose up -d db
   for f in migrations/000*.sql; do
     echo "Applying $f"
     docker compose exec -T db \
       psql -U leadgen -d leadgen -v ON_ERROR_STOP=1 -f - < "$f"
   done
   # если БД и юзер другие — подставьте свои значения
   ```
3. **Запустите сервисы:**
   ```bash
   docker compose up --build
   ```
7. **Очередь рассылки.** `EmailSender.queue` сохраняет результат генерации в `outreach_messages` со статусом `scheduled`, добавляя случайную задержку 4–8 минут относительно предыдущего письма (с блокировкой последнего `scheduled_for`, чтобы параллельные воркеры не нарушали интервал) и гарантируя, что `scheduled_for` попадает в окно 09:10–19:45 по МСК. Email и JSON-запрос LLM кладутся в `metadata`, чтобы можно было восстановить, что именно отправляется.

Для подключения к БД из DBeaver или локального `psql` используй внешний порт `POSTGRES_EXPOSE_PORT` из `.env`. Внутри docker-compose сервисы приложения всегда подключаются к `db:5432`, поэтому `POSTGRES_PORT` менять не нужно.

## Полезные команды

- Запуск синхронизации Google Sheets вручную:
  ```bash
  docker compose run --rm app python -m app.tools.sync_sheet --batch-tag <tag>
  ```
- Просмотр очереди писем:
  ```bash
  docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "SELECT id, scheduled_for, status FROM outreach_messages ORDER BY scheduled_for LIMIT 20;"
  ```
- Перепланировать очередь с задержкой (пример интерактивного скрипта):
  ```bash
  docker compose run --rm app python - <<'PY'
  import random
  from datetime import datetime, timedelta, timezone
  from zoneinfo import ZoneInfo
  from sqlalchemy import text
  from app.modules.send_email import EmailSender
  from app.modules.utils.db import get_session_factory

  sender = EmailSender()
  tz = ZoneInfo(sender.timezone_name)
  current = datetime.now(tz)
  session = get_session_factory()()
  rows = session.execute(text("SELECT id FROM outreach_messages WHERE status='scheduled' ORDER BY created_at"))
  for row in rows.mappings():
      current += timedelta(seconds=random.randint(540, 960))
      session.execute(text("UPDATE outreach_messages SET scheduled_for = :ts WHERE id = :id"),
                      {"ts": current.astimezone(timezone.utc), "id": row["id"]})
  session.commit()
  PY
  ```
- Переотправка письма вручную:
  ```bash
  docker compose run --rm app python - <<'PY'
  from app.modules.send_email import EmailSender
  sender = EmailSender()
  sender.deliver(
      outreach_id="<uuid>",
      company_id="<company uuid>",
      contact_id="<contact uuid>",
      to_email="test@example.com",
      subject="Тест",
      body="Тестовое письмо"
  )
  PY
  ```
- Очистка очереди от невалидных адресов (телефоны, пустые строки). Скрипт помечает такие письма как `skipped` и удаляет «email»-контакты без `@`:
  ```bash
  docker compose exec db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -f docs/sql/cleanup-invalid-emails.sql
  ```

### Чек-лист ручной проверки MX-роутинга

1. Подготовьте по одному адресу с MX Яндекса (`@yandex.ru`), Mail.ru (`@mail.ru`) и Google (`@gmail.com`), а также адрес с намеренно несуществующим доменом.
2. В очереди (`outreach_messages`) выставьте `scheduled_for` в текущий час и запустите `EmailSender.deliver` (см. команды выше).
3. Убедитесь, что письма на Яндекс/Мейл.ру ушли через Яндекс SMTP (`metadata.route.provider = "yandex"`, заголовок `From` совпадает с `YANDEX_FROM`, `Reply-To` отсутствует — ответы пойдут в этот же ящик).
4. Проверьте, что письма на остальные домены и адрес с ошибкой DNS отправлены через Gmail с `metadata.mx.class = "OTHER"`/`"UNKNOWN"`.
5. Протестируйте случай с неверным паролем приложения Яндекса: временно измените `YANDEX_PASS`, перезапустите доставку и убедитесь, что письмо не отправилось (статус `failed`, `metadata.route.error` содержит ответ SMTP).
6. Если во время отправки через Яндекс приходит ошибка вида `5.7.x Message rejected under suspicion of SPAM`, сервис автоматически переключится на Gmail (`metadata.route.provider = "gmail"`, `metadata.route.fallback = true`) и сохранит текст ошибки в `metadata.route.error`.

## Деплой на удалённом сервере через Git

1. **Подготовка сервера:** установите Docker и docker compose plugin, создайте отдельного пользователя без root.
2. **Клонируйте репозиторий:**
   ```bash
   git clone https://github.com/kodjooo/lead-generation-wine.git
   cd lead-generation-wine
   cp .env.example .env
   ```
3. **Подготовьте каталог с секретами:**
   ```bash
   mkdir -p secure
   ```
4. **Заполните `.env`:**
   - пропишите ключи Yandex и Google;
   - укажите `POSTGRES_EXPOSE_PORT`, если нужен доступ к БД с хоста через DBeaver/psql;
   - заполните Gmail `GMAIL_*` (App Password) и/или Яндекс `YANDEX_*`;
   - для первого прогона безопаснее поставить:
     ```env
     EMAIL_GENERATION_ENABLED=false
     EMAIL_SENDING_ENABLED=false
     ```
   - после проверки пайплайна включить нужные флаги обратно.
5. **Разместите ключи сервисных аккаунтов:** скопируйте файлы JSON в каталог `secure/` на сервере.
   Ожидаемые пути:
   - `secure/authorized_key.json`
   - `secure/google-credentials.json`

   Если вместо файла по этим путям окажется директория, сервисы завершатся ошибкой `IsADirectoryError`.
6. **Проверьте конфиг до запуска:**
   ```bash
   grep -E '^(POSTGRES_HOST|POSTGRES_PORT|POSTGRES_EXPOSE_PORT|POSTGRES_DB|POSTGRES_USER|GOOGLE_SHEET_ID|GOOGLE_SHEET_TAB|EMAIL_GENERATION_ENABLED|EMAIL_SENDING_ENABLED)=' .env
   ```
   Для контейнеров должно быть:
   - `POSTGRES_HOST=db`
   - `POSTGRES_PORT=5432`

   А внешний порт для DBeaver/psql задаётся отдельно через `POSTGRES_EXPOSE_PORT`.
7. **Соберите образ и поднимите только инфраструктуру:**
   ```bash
   docker compose pull
   docker compose up -d db redis
   ```
8. **Примените миграции:**
   ```bash
   for f in migrations/000*.sql; do
     echo "Applying $f"
     docker compose exec -T db \
       psql -U leadgen -d leadgen -v ON_ERROR_STOP=1 -f - < "$f"
   done
   # замените leadgen/leadgen на свои POSTGRES_USER/POSTGRES_DB при необходимости
   ```
9. **Запустите сервисы:**
   ```bash
   docker compose up -d --build
   ```
   Хостовой Redis останавливать не нужно: контейнерный Redis работает только внутри сети compose и не занимает порт `6379` на сервере.
10. **Проверьте, что контейнеры здоровы:**
   ```bash
   docker compose ps
   docker compose logs --tail=50 app
   docker compose logs --tail=50 scheduler
   docker compose logs --tail=50 worker
   ```
11. **Сделайте первый ручной sync из Google Sheets:**
   ```bash
   docker compose run --rm app python -m app.tools.sync_sheet
   ```
   После этого можно проверить, что запросы реально появились:
   ```bash
   docker compose exec -T db psql -U leadgen -d leadgen -P pager=off -c "select count(*) from serp_queries;"
   ```
12. **Если нужен немедленный тестовый прогон без ожидания `scheduled_for`:**
   ```bash
   docker compose exec -T db psql -U leadgen -d leadgen -c "update serp_queries set scheduled_for = now(), status = 'pending' where status = 'pending';"
   docker compose logs -f scheduler
   ```
13. **Обновление проекта из репозитория:**
   ```bash
   git pull
   docker compose up -d db redis
   for f in migrations/000*.sql; do
     echo "Applying $f"
     docker compose exec -T db \
       psql -U leadgen -d leadgen -v ON_ERROR_STOP=1 -f - < "$f"
   done
   docker compose up -d --build
   ```
14. **Быстрый rollback по коду:** при необходимости вернитесь на предыдущий commit и пересоберите контейнеры.
15. **Мониторинг:**
   ```bash
   docker compose logs -f app
   docker compose logs -f worker
   docker compose logs -f scheduler
   ```

### Управление оркестратором

Запустить оркестратор однократно:

```bash
docker compose run --rm app --mode once
```

Фоновый режим по умолчанию (`loop`) запускается в контейнерах `app`, `scheduler`, `worker` при `docker compose up`.

## Переменные окружения

### Yandex Cloud

- `YANDEX_CLOUD_FOLDER_ID` — ID каталога (консоль YC → «Обзор»).
- `YANDEX_CLOUD_IAM_TOKEN` — можно оставить пустым; при наличии ключа сервисного аккаунта пайплайн возьмёт токен автоматически.
- `YANDEX_CLOUD_SA_KEY_FILE` / `YANDEX_CLOUD_SA_KEY_JSON` — путь или содержимое ключа сервисного аккаунта. Получить ключ:

  ```bash
  yc iam key create --service-account-name <sa_name> --output key.json
  ```

  Ключ храните в Secret Manager или CI и не коммитьте в репозиторий.
- `YANDEX_ENFORCE_NIGHT_WINDOW` — если `true`, отправка запросов выполняется только в ночное окно; установите `false` для дневных тестов.
- `YANDEX_RESULTS_PROCESSING_MODE` — режим обработки готовых deferred-результатов:
  `anytime` — polling и ingest можно выполнять в любое время;
  `night_only` — polling и ingest выполняются только ночью по `APP_TIMEZONE`.

### Google Sheets

- `GOOGLE_SHEET_ID` — идентификатор таблицы со списком городов.
- `GOOGLE_SHEET_TAB` — имя вкладки со столбцами `city`, `country`, `batch_tag`, `search_malls`, `search_agencies`.
- `GOOGLE_SA_KEY_FILE` / `GOOGLE_SA_KEY_JSON` — ключ сервисного аккаунта Google с доступом на чтение/редактирование таблицы.
- `SHEET_SYNC_ENABLED` — включает автоматическую синхронизацию (true/false).
- `SHEET_SYNC_INTERVAL_MINUTES` — период автосинхронизации (мин., по умолчанию 60).
- `SHEET_SYNC_BATCH_TAG` — опциональный фильтр по партии.

### Email и OpenAI

- `YANDEX_SMTP_HOST`, `YANDEX_SMTP_PORT`, `YANDEX_SMTP_SSL`, `YANDEX_USER`, `YANDEX_PASS`, `YANDEX_FROM` — отправка через Яндекс SMTP (пароль приложения в Яндекс Почте → Настройки → Пароли приложений).
- Для доставки писем используется только Яндекс SMTP; настройки Gmail SMTP и MX-маршрутизация больше не требуются.
- `EMAIL_GENERATION_ENABLED` — если `false`, оркестратор не будет генерировать и ставить письма в `outreach_messages`.
- `EMAIL_SENDING_ENABLED` — если `false`, письма могут сохраняться в `outreach_messages` со статусом `scheduled`, но реальная отправка отключена.
- `OPENAI_API_KEY` — ключ OpenAI для генерации персонализированных писем.
- `SITE_CLASSIFICATION_LLM_ENABLED` — включает OpenAI fallback для спорных кандидатов после homepage-проверки.
- `SITE_CLASSIFICATION_LLM_MODEL` — модель OpenAI для fallback-классификации типа сайта и фактического города.
- `SITE_CLASSIFICATION_LLM_MIN_CONFIDENCE` — минимальная уверенность fallback-ответа, после которой verdict используется для финального решения.

## Синхронизация запросов из Google Sheets

1. Заполните таблицу на листе городов (столбцы `city`, `country`, `batch_tag`, `search_malls`, `search_agencies`).
2. Выполните синхронизацию:

   ```bash
   docker compose run --rm app python -m app.tools.sync_sheet
   # или выбрать конкретную партию
   docker compose run --rm app python -m app.tools.sync_sheet --batch-tag batch-2025-10
   ```

   Скрипт создаст записи в `serp_queries` и обновит служебные колонки листа (`status`, `generated_count` и т.д.).

3. При установке `SHEET_SYNC_ENABLED=true` оркестратор автоматически вызывает синхронизацию каждые `SHEET_SYNC_INTERVAL_MINUTES` минут, используя тот же CLI-процесс под капотом.

## Миграции БД

После обновления проекта выполните SQL-миграции:

```bash
for f in migrations/000*.sql; do
  echo "Applying $f"
  docker compose exec -T db \
    psql -U leadgen -d leadgen -v ON_ERROR_STOP=1 -f - < "$f"
done
# если переменные отличаются, замените leadgen/leadgen на свои POSTGRES_USER/DB
```

## Развёртывание на удалённом сервере

См. раздел «Деплой на удалённом сервере через Git» выше — там перечислены все шаги (клонирование репозитория, заполнение `.env`, миграции и запуск). Дополнительно рекомендуется настроить:

- автоматический старт с помощью systemd unit, если сервер перезагружается;
- регулярные бэкапы каталога `pg_data` и файла `.env`;
- централизованный сбор логов (`docker compose logs`, Loki, ELK и т.д.).

## Тестирование

```bash
docker compose run --rm app python -m pytest
```
4. **Проверка рассылки:** убедитесь, что `EMAIL_SENDING_ENABLED=true`, а текущее время попадает в окно 09:10–19:45 (МСК). Для ручного теста можно изменить `scheduled_for` конкретной записи; учитывайте, что новые письма автоматически разнесены на 4–8 минут от предыдущего.
## Повторная LLM-классификация

Если gateway/LLM был включён позже основного пайплайна, можно дозаполнить `llm_*` для уже найденных компаний без повторного Yandex-поиска и без повторного обхода сайтов:

```bash
docker compose run --rm app python -m app.tools.recheck_llm_sites --limit 500
```

Полезные флаги:
- `--retry-errors` — повторно брать компании, уже помеченные `llm_status=error`
- `--dry-run` — только показать кандидатов и ответы без записи в БД

Команда берёт компании с заполненным `companies.attributes.homepage_excerpt`, у которых ещё нет успешного `llm_status=success`, отправляет их через тот же LLM/gateway-контур и дозаписывает `llm_status`, `llm_provider`, `llm_checked_at`, `llm_site_verdict`, `llm_confidence`, `llm_reason`. Если LLM вернул `detected_city`, обновляется и `actual_region`.
