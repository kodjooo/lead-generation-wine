ТЗ: сервис LLM gateway для site classification

1. Цель
Сервис нужен для того, чтобы основной pipeline на прод-сервере не ходил в OpenAI напрямую.
Основной сервер должен отправлять в gateway только данные для классификации спорного сайта, а gateway уже сам обращается в OpenAI из разрешенной юрисдикции и возвращает нормализованный результат.

2. Что должен делать сервис
- Принимать HTTP POST запросы от основного pipeline.
- Проверять Bearer-токен доступа.
- Вызывать OpenAI Chat Completions с заданной моделью.
- Возвращать унифицированный JSON-ответ с результатом site classification.
- Логировать ошибки, таймауты и входные request_id без хранения чувствительных данных дольше необходимого.
- Поддерживать healthcheck для мониторинга.

3. Обязательный endpoint
POST /v1/site-classification

4. Healthcheck
GET /health
Ответ 200:
{
  "status": "ok"
}

5. Авторизация
- Основной способ: заголовок Authorization: Bearer <token>
- Токен хранится на стороне pipeline в переменной SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY
- Без корректного токена сервис возвращает 401

6. Входной JSON для POST /v1/site-classification
{
  "schema": "site_classification_v1",
  "model": "gpt-5-mini",
  "input": {
    "expected_city": "Краснодар",
    "expected_entity_type": "real_estate_agency",
    "domain": "verno.pro",
    "serp": {
      "title": "VERNO",
      "snippet": "Недвижимость в Краснодаре",
      "url": "https://verno.pro/",
      "position": 3
    },
    "serp_screening": {
      "score": 2.5,
      "reason": "serp_needs_homepage_verification",
      "requires_verification": true
    },
    "homepage_screening": {
      "score": 5.0,
      "reason": null
    },
    "homepage_excerpt": "Каталог объектов..."
  }
}

7. Выходной JSON
Успешный ответ 200:
{
  "site_verdict": "official_real_estate_agency_site",
  "detected_city": "Краснодар",
  "confidence": 0.94,
  "reason": "Сайт агентства недвижимости с каталогом объектов и контактами.",
  "provider": "openai",
  "model": "gpt-5-mini"
}

8. Допустимые значения site_verdict
- official_mall_site
- mall_tenant_site
- official_real_estate_agency_site
- developer_site
- aggregator_or_directory
- media_or_article
- uncertain
- null

9. Ошибки
- 400: невалидный JSON или неизвестная schema
- 401: неверный или отсутствующий токен
- 429: локальный rate limit gateway
- 502: OpenAI вернул ошибку, которую gateway не смог обработать
- 504: таймаут вызова OpenAI

Формат ошибки:
{
  "error": {
    "code": "upstream_timeout",
    "message": "OpenAI request timed out"
  }
}

10. Требования к логике gateway
- Gateway не должен возвращать сырой ответ OpenAI.
- Gateway должен сам парсить ответ модели и отдавать только нормализованный JSON.
- Gateway должен валидировать, что confidence приведен к float, а site_verdict входит в допустимый список.
- При невалидном ответе OpenAI gateway должен вернуть 502.
- Таймаут одного вызова OpenAI: 45 секунд.
- Повторы на временных ошибках OpenAI: до 3 попыток с backoff 1s, 2s, 3s.

11. Требования к безопасности
- Не логировать Authorization header.
- Не логировать полный homepage_excerpt целиком; максимум первые 500 символов в debug.
- Ограничить входной payload по размеру, например 32 KB.
- Разрешить доступ только с IP основного сервера или через private network/VPN, если возможно.

12. Требования к развертыванию
- Небольшой отдельный сервис на FastAPI/Flask.
- Отдельный env:
  - OPENAI_API_KEY
  - GATEWAY_AUTH_TOKEN
  - DEFAULT_OPENAI_MODEL
  - LOG_LEVEL
- Развертывание в стране/юрисдикции, где OpenAI API доступен.
- Запуск через systemd или Docker Compose.

13. Требования к наблюдаемости
- Метрики:
  - requests_total
  - requests_failed_total
  - upstream_openai_latency_ms
  - upstream_openai_errors_total
- Логи:
  - request_id
  - status_code
  - model
  - duration_ms
- Желательно добавить Sentry или аналогичный error tracker.

14. Требования к совместимости с текущим pipeline
- Pipeline уже умеет работать в режиме SITE_CLASSIFICATION_LLM_PROVIDER=gateway.
- Для включения gateway достаточно выставить:
  - SITE_CLASSIFICATION_LLM_ENABLED=true
  - SITE_CLASSIFICATION_LLM_PROVIDER=gateway
  - SITE_CLASSIFICATION_LLM_GATEWAY_URL=https://<gateway-host>
  - SITE_CLASSIFICATION_LLM_GATEWAY_API_KEY=<token>
- OPENAI_API_KEY на основном прод-сервере при этом не нужен для site classification.

15. Критерии приемки
- Основной pipeline успешно вызывает gateway вместо OpenAI напрямую.
- На основном проде отсутствуют ошибки OpenAI 403 unsupported_country_region_territory.
- Gateway корректно возвращает классификацию минимум для 10 тестовых доменов.
- При временном падении OpenAI gateway делает retry и не ломает pipeline.
- При недоступности gateway pipeline логирует предупреждение и продолжает работу без LLM verdict.
