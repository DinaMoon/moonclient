# MoonClient 🌙

<div align="center">

[![Crates.io](https://img.shields.io/crates/v/moonclient.svg?style=flat-square)](https://crates.io/crates/moonclient)
[![Documentation](https://docs.rs/moonclient/badge.svg?style=flat-square)](https://docs.rs/moonclient)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg?style=flat-square)](https://www.rust-lang.org)

[English](README.md) | **Русский**

Универсальный, отказоустойчивый и высокопроизводительный HTTP-движок для критически важных API-клиентов, распределенных парсеров и асинхронных микросервисов.

</div>

---

## 📑 Оглавление

- [Обзор](#-обзор)
- [Ключевые возможности](#-ключевые-возможности)
- [Установка](#-установка)
- [Быстрый старт](#-быстрый-старт)
- [Ключевые концепции](#-ключевые-концепции)
  - [1. Хуки жизненного цикла (`ClientHook`)](#1-хуки-жизненного-цикла-clienthook)
  - [2. Ограничение частоты (Token Bucket Rate Limiting)](#2-ограничение-частоты-token-bucket-rate-limiting)
  - [3. Защита от OOM и лимиты памяти буфера](#3-защита-от-oom-и-лимиты-памяти-буфера)
  - [4. Безопасная сборка URL без аллокаций (`build_url!`)](#4-безопасная-сборка-url-без-аллокаций-build_url)
  - [5. Раздельное файловое логирование (`MoonLogger`)](#5-раздельное-файловое-логирование-moonlogger)
  - [6. Интернационализация на базе Project Fluent (`i18n`)](#6-интернационализация-на-базе-project-fluent-i18n)
- [Флаги компиляции (Features)](#-флаги-компиляции-features)
- [Архитектура и реэкспорты](#-архитектура-и-реэкспорты)
- [Лицензия](#-лицензия)

---

## 🌟 Обзор

`moonclient` спроектирован для полного устранения повторяющегося рутинного кода при создании надежных production-библиотек сетевых клиентов на Rust. Библиотека оборачивает `reqwest` и `reqwest-middleware` в чистое, потокобезопасное ядро, полностью изолируя транспортный уровень, политики повторов, rate-лимиты и безопасность памяти от прикладной бизнес-логики конечных API.

---

## ⚡ Ключевые возможности

- 🛡️ **Автономный Rate Limiting:** Встроенный алгоритм Token Bucket на базе `governor`: тонкая настройка множества произвольных временных окон (`Duration`), готовые пресеты RPS/RPM и распределенная координация через Redis.
- 🔁 **Отказоустойчивый конвейер Middleware:** Прозрачные автоматические повторы с экспоненциальным бэкоффом (`reqwest-retry`), спроектированные для безопасного пропуска неклонируемых multipart-запросов и файлов без паник.
- 🪝 **Хуки жизненного цикла:** Трейт-интерцептор ([`ClientHook`]), позволяющий динамически подмешивать заголовки авторизации (Bearer-токены, HMAC) и реактивно обрабатывать `401 Unauthorized` с фоновым обновлением сессии и повтором запроса.
- 🔒 **Защита от OOM (Out-Of-Memory):** Буферизация ответов с жестким ограничением максимального объема памяти, предотвращающая падение процесса при получении аномально огромных ответов или HTML-страниц сбоев.
- 📦 **Паттерн экстракторов:** Единый метод `.execute()`, автоматически десериализующий ответ в `Json<T>`, `Xml<T>`, потоковый `Response` или пустой `()` (void).
- 🌍 **Нативная локализация Project Fluent:** Встроенная многоязычная система диагностических сообщений и ошибок сети с потокобезопасным Task-Local определением активного языка.
- 🚀 **Нулевые аллокации при сборке путей:** Макрос [`build_url!`] на лету приводит любые типы данных и перечисления к сегментам URL через `Cow<'_, str>`.

---

## 📦 Установка

Добавьте `moonclient` в ваш `Cargo.toml`:

```toml
[dependencies]
moonclient = "0.1"
tokio = { version = "1.53", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

---

## 🚀 Быстрый старт

Простой пример инициализации клиента с ограничением частоты запросов и извлечением типизированного JSON:

```rust
use moonclient::response::Json;
use moonclient::{ClientHook, MoonClient, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Todo {
    id: u64,
    title: String,
    completed: bool,
}

// Минимальный хук-заглушка по умолчанию
struct DefaultHook;

#[async_trait::async_trait]
impl ClientHook for DefaultHook {
    type Error = std::convert::Infallible;
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Создаем клиент с лимитом 5 запросов в секунду
    let client = MoonClient::builder(DefaultHook)
        .with_base_url("https://jsonplaceholder.typicode.com")
        .requests_per_second(5)
        .build();

    // 2. Безопасно формируем URL эндпоинта
    let url = moonclient::build_url!(client, "todos", 1)?;
    let request = client.get(url);

    // 3. Отправляем запрос и извлекаем JSON-модель
    let Json(todo): Json<Todo> = client.execute(request).await?;

    println!("Получена задача #{}: {} (статус: {})", todo.id, todo.title, todo.completed);
    Ok(())
}
```

---

## 🧠 Ключевые концепции

### 1. Хуки жизненного цикла (`ClientHook`)

`MoonClient` изолирует авторизацию и особенности конкретных API через трейт [`ClientHook`]:

```rust
use async_trait::async_trait;
use moonclient::ClientHook;
use reqwest_middleware::RequestBuilder;
use reqwest::Response;

struct MyAuthHook {
    token: String,
}

#[async_trait]
impl ClientHook for MyAuthHook {
    type Error = std::convert::Infallible;

    // Выполняется непосредственно перед отправкой запроса в сеть
    async fn pre_request(&self, request: RequestBuilder) -> Result<RequestBuilder, Self::Error> {
        Ok(request.header("Authorization", format!("Bearer {}", self.token)))
    }

    // Перехватывает 401 Unauthorized для фонового обновления токенов
    async fn handle_unauthorized(&self) -> Result<bool, Self::Error> {
        // Асинхронно обновляем сессию...
        Ok(true) // Возвращаем true для автоматического перезапуска оригинального запроса
    }
}
```

### 2. Ограничение частоты (Token Bucket Rate Limiting)

Лимитирование запросов применяется прозрачно на уровне ядра до выхода в сеть:

```rust
let client = MoonClient::builder(DefaultHook)
    .add_limit(5, Duration::from_millis(200)) // Custom window: 5 requests per 200ms
    .requests_per_second(5)                   // Quick preset: 5 RPS
    .requests_per_minute(90)                  // Quick preset: 90 RPM
    .build();
```

Для распределенных микросервисов включите фичу `redis-limit` для синхронизации квот через Redis.

### 3. Защита от OOM и лимиты памяти буфера

Стандартные HTTP-библиотеки считывают тело ответа в память целиком. Если удаленный сервер вернет непрерывный поток данных или HTML-страницу сбоя на 500 МБ, приложение упадет по Out-Of-Memory (OOM).

`moonclient` считывает поток с соблюдением строгих емкостных лимитов:
- **`DEFAULT_ERROR_BODY_LIMIT` (64 КБ):** Для тел ошибок HTTP 4xx/5xx.
- **`DEFAULT_BODY_LIMIT` (16 МБ):** Для успешных ответов API.
- **`set_max_response_size(bytes)`:** Динамическая регулировка порога в рантайме через атомарный счетчик.

### 4. Безопасная сборка URL без аллокаций (`build_url!`)

Макрос `build_url!` автоматически конвертирует любые типы данных, реализующие [`IntoSegment`]:

```rust
let user_id = 42;
let route = "profile";

// Результат: https://api.site.com/users/42/profile
let url = moonclient::build_url!(client, "users", user_id, route)?;
```

### 5. Раздельное файловое логирование (`MoonLogger`)

`MoonLogger` обеспечивает неблокирующее многопоточное логирование с автоматическим восстановлением при отравлении мьютексов (Mutex Poisoning) и раздельной маршрутизацией:

```rust
moonclient::logger::init()
    .with_level(log::LevelFilter::Debug)
    .with_split_files("logs/core.log", "logs/api.log")
    .setup()?;
```
- Логи с целью `moonclient*` направляются в `core.log`.
- Логи внешних прикладных API направляются в `api.log`.

### 6. Интернационализация на базе Project Fluent (`i18n`)

Все внутренние сообщения об ошибках сети и системные логи локализованы через **Project Fluent**:
- Автоматическая иерархия опроса: Tokio Task-Local ➡️ Глобальная настройка процесса ➡️ Язык ОС хоста ➡️ Язык по умолчанию (`en`).
- Внешние крейты могут регистрировать собственные словари через `moonclient::i18n::register_resource()`.

---

## 🚩 Флаги компиляции (Features)

| Флаг | Описание | По умолчанию |
| :--- | :--- | :---: |
| `xml` | Включает экстрактор десериализации XML-ответов на базе `quick-xml`. | **Отключен** |
| `redis-limit` | Включает распределенный rate limiting через пулы соединений Redis. | **Отключен** |

---

## 🏗️ Архитектура и реэкспорты

Чтобы избежать расхождения версий транзитивных зависимостей в вашем рабочем пространстве (workspace), `moonclient` напрямую реэкспортирует используемый сетевой стек:

```rust
pub use moonclient::reqwest;
pub use moonclient::reqwest_middleware;
pub use moonclient::reqwest_tracing;
```

---

## 📄 Лицензия

Лицензируется по вашему выбору под:

- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) или <http://www.apache.org/licenses/LICENSE-2.0>)
- **MIT license** ([LICENSE-MIT](LICENSE-MIT) или <http://opensource.org/licenses/MIT>)
