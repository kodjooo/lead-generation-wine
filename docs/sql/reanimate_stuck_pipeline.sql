-- Реанимация stuck deferred-запросов и частичный повторный прогон enrichment.
-- Запускать только после деплоя кода с разделением ролей app/worker
-- и исправленной retry-логикой для serp_operations / serp_queries.

BEGIN;

-- 1. Вернуть в pending те запросы, у которых последняя операция упала
--    по временной причине и лимит retry еще не исчерпан.
WITH latest_failed_ops AS (
    SELECT DISTINCT ON (so.query_id)
        so.query_id,
        so.retry_count,
        so.error_payload
    FROM serp_operations so
    WHERE so.status = 'failed'
    ORDER BY so.query_id, so.requested_at DESC
)
UPDATE serp_queries sq
SET status = 'pending',
    updated_at = NOW()
FROM latest_failed_ops lfo
WHERE sq.id = lfo.query_id
  AND sq.status = 'in_progress'
  AND lfo.retry_count < 3
  AND COALESCE(lfo.error_payload ->> 'reason', '') NOT LIKE '%404%';

-- 2. Пометить failed те запросы, у которых последняя операция завершилась
--    терминальной ошибкой 404 или уже исчерпала retry_count.
WITH latest_failed_ops AS (
    SELECT DISTINCT ON (so.query_id)
        so.query_id,
        so.retry_count,
        so.error_payload
    FROM serp_operations so
    WHERE so.status = 'failed'
    ORDER BY so.query_id, so.requested_at DESC
)
UPDATE serp_queries sq
SET status = 'failed',
    updated_at = NOW()
FROM latest_failed_ops lfo
WHERE sq.id = lfo.query_id
  AND sq.status = 'in_progress'
  AND (
      lfo.retry_count >= 3
      OR COALESCE(lfo.error_payload ->> 'reason', '') LIKE '%404%'
  );

-- 3. Опционально вернуть в new часть компаний без контактов, у которых
--    enrichment когда-то завершился contacts_not_found, но вы хотите
--    повторно прогнать их уже после фикса crawler.
-- Снимите комментарий при необходимости.
--
-- UPDATE companies
-- SET status = 'new',
--     primary_email = NULL,
--     primary_email_status = NULL,
--     primary_email_note = NULL,
--     updated_at = NOW()
-- WHERE status = 'contacts_not_found';

COMMIT;
