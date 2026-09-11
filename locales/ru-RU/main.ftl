# --- Timeouts ---
timeout-changed = ⏱️ Сетевой таймаут изменен на: {$timeout}
timeout-disabled = ⏱️ Сетевой таймаут полностью отключен.

# --- Limits ---
limit-rule-changed = 📡 Правило лимитов успешно изменено: {$requests} req / {$window}
limit-invalid-ignored = 📡 Игнорируется невалидный лимит при пакетной настройке: {$count} req / {$duration}
limit-strategy-reset = 📡 Вся стратегия лимитирования частоты успешно перезаписана.

# --- Redis Distributed Limiter ---
redis-limit-waiting = 📡 Достигнут глобальный лимит для ключа "{$key}". Задерживаем выполнение, ожидание {$duration} мс...
redis-limit-error = ❌ Не удалось связаться с распределенным лимитером Redis: {$error}

# --- Network ---
network-request-sending = Отправка запроса по сетевому адресу: "{$url}"
network-response-received = Получен ответ от сетевого адреса: "{$url}"
network-download-started = Загрузка бинарного файла по адресу: {$url}

# --- Network Retries ---
request-retrying = 🔄 Запуск повторной попытки запроса...

# --- Panics / Invariant Expects ---
expect-invalid-base-url = Базовый URL клиента сформирован неверно. Пожалуйста, проверьте настройки.
expect-limit-greater-than-zero = Внутренний инвариант: лимит частоты запросов должен быть больше нуля.
expect-invalid-period-calculation = Внутренний инвариант: неверный расчет периода.
expect-invalid-macro-base-url = Невалидный базовый URL клиента. Пожалуйста, проверьте настройки инициализации.

# --- System Errors ---
err-middleware = Ошибка Middleware: {$error}
err-network = Ошибка сети: {$error}
err-parse = Ошибка парсинга JSON: {$error}
err-io = Ошибка ввода-вывода (IO): {$error}
err-api-validation = Ошибка валидации API: {$error}
err-api-error = Сервер вернул ошибку: {$code} - {$message}
err-config = Ошибка конфигурации клиента: {$error}
err-response-decode = Ошибка декодирования ответа API: {$error}\nСырой ответ сервера:\n{$raw}
err-unauthorized = Локальная ошибка авторизации: {$error}
err-validation-error = Ошибка валидации: {$error}
err-unknown = Неизвестная ошибка
