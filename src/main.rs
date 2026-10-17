use serde::{Deserialize, Serialize};
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path as FsPath, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{Manager, State};

const CONFIG_FILE: &str = "connections.json";

#[derive(Clone)]
struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    operations: Mutex<()>,
    config: Mutex<()>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Connection {
    id: String,
    name: String,
    uri: String,
}

#[derive(Serialize)]
struct PublicConnection {
    id: String,
    name: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct BackupMeta {
    id: String,
    name: String,
    created_at: u128,
}

#[derive(Serialize)]
struct Backup {
    id: String,
    name: String,
    created_at: u128,
    bytes: u64,
}

#[derive(Deserialize, Serialize)]
struct DatabaseStatus {
    database: String,
    user: String,
    version: String,
    size: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
}

impl ApiError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    fn bad(message: impl Into<String>) -> Self {
        Self::new(message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(message)
    }
}

async fn blocking<T, F>(work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ApiError> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(work)
        .await
        .map_err(|error| format!("Фоновая операция завершилась: {error}"))?
        .map_err(|error| error.message)
}

async fn serialized<T, F>(inner: Arc<Inner>, work: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&FsPath) -> Result<T, ApiError> + Send + 'static,
{
    blocking(move || {
        let _guard = inner
            .operations
            .lock()
            .map_err(|error| ApiError::internal(format!("Не удалось получить блокировку: {error}")))?;
        work(&inner.root)
    })
    .await
}

fn main() {
    eprintln!("PG Snapshots: starting native window");
    tauri::Builder::default()
        .setup(|app| {
            let root = app.path().app_local_data_dir().map_err(|error| {
                eprintln!("PG Snapshots: could not resolve app data directory: {error}");
                error
            })?;
            let mut legacy_sources = Vec::new();
            if let Some(parent) = std::env::current_exe()?.parent() {
                legacy_sources.push(parent.to_path_buf());
            }
            legacy_sources.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
            initialize_data(&root, &legacy_sources).map_err(|error| {
                eprintln!("PG Snapshots: could not initialize local data: {error}");
                std::io::Error::other(error)
            })?;
            app.manage(AppState {
                inner: Arc::new(Inner {
                    root,
                    operations: Mutex::new(()),
                    config: Mutex::new(()),
                }),
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_connections,
            add_connection,
            connection_status,
            list_backups,
            create_backup,
            reset_database_command,
            delete_backup,
            restore_backup,
            get_data_directory,
            open_data_directory
        ])
        .run(tauri::generate_context!())
        .expect("не удалось запустить PG Snapshots");
}

#[tauri::command]
fn list_connections(state: State<'_, AppState>) -> Result<Vec<PublicConnection>, String> {
    load_connections(&state.inner.root)
        .map(|connections| connections.into_iter().map(|connection| PublicConnection {
            id: connection.id,
            name: connection.name,
        }).collect())
        .map_err(|error| error.message)
}

#[tauri::command]
async fn add_connection(
    state: State<'_, AppState>,
    name: String,
    uri: String,
) -> Result<PublicConnection, String> {
    let name = name.trim().to_string();
    let uri = uri.trim().to_string();
    if name.is_empty() || name.chars().count() > 80 || name.chars().any(char::is_control) {
        return Err("Введите название длиной до 80 символов.".to_string());
    }
    validate_uri(&uri).map_err(|error| error.message)?;
    let id = stable_id(&uri);
    let inner = state.inner.clone();
    blocking(move || {
        let _guard = inner.config.lock()
            .map_err(|error| ApiError::internal(format!("Не удалось получить блокировку: {error}")))?;
        let mut connections = load_connections(&inner.root)?;
        if let Some(existing) = connections.iter_mut().find(|connection| connection.id == id) {
            if existing.uri != uri {
                return Err(ApiError::internal("Коллизия идентификатора подключения."));
            }
            existing.name = name.clone();
        } else {
            connections.push(Connection { id: id.clone(), name: name.clone(), uri });
        }
        save_connections(&inner.root, &connections)?;
        Ok(PublicConnection { id, name })
    }).await
}

#[tauri::command]
async fn connection_status(state: State<'_, AppState>, id: String) -> Result<DatabaseStatus, String> {
    let connection = find_connection(&state.inner.root, &id).map_err(|error| error.message)?;
    blocking(move || database_status(&connection.uri)).await
}

#[tauri::command]
async fn list_backups(state: State<'_, AppState>, id: String) -> Result<Vec<Backup>, String> {
    validate_connection_id(&id).map_err(|error| error.message)?;
    let root = state.inner.root.clone();
    blocking(move || read_backups(&root, &id)).await
}

#[tauri::command]
async fn create_backup(
    state: State<'_, AppState>,
    id: String,
    name: String,
) -> Result<Backup, String> {
    validate_connection_id(&id).map_err(|error| error.message)?;
    let name = name.trim().to_string();
    if name.is_empty() || name.chars().count() > 100 || name.chars().any(char::is_control) {
        return Err("Введите название снимка длиной до 100 символов.".to_string());
    }
    let connection = find_connection(&state.inner.root, &id).map_err(|error| error.message)?;
    let inner = state.inner.clone();
    serialized(inner, move |root| make_backup(root, &connection, &name)).await
}

#[tauri::command]
async fn reset_database_command(state: State<'_, AppState>, id: String) -> Result<String, String> {
    validate_connection_id(&id).map_err(|error| error.message)?;
    let connection = find_connection(&state.inner.root, &id).map_err(|error| error.message)?;
    let inner = state.inner.clone();
    serialized(inner, move |root| reset_database(root, &connection)).await
}

#[tauri::command]
async fn delete_backup(
    state: State<'_, AppState>,
    id: String,
    backup_id: String,
) -> Result<String, String> {
    validate_connection_id(&id).map_err(|error| error.message)?;
    validate_backup_id(&backup_id).map_err(|error| error.message)?;
    let inner = state.inner.clone();
    serialized(inner, move |root| {
        remove_backup(root, &id, &backup_id)?;
        Ok("Снимок удалён.".to_string())
    }).await
}

#[tauri::command]
async fn restore_backup(
    state: State<'_, AppState>,
    id: String,
    backup_id: String,
) -> Result<String, String> {
    validate_connection_id(&id).map_err(|error| error.message)?;
    validate_backup_id(&backup_id).map_err(|error| error.message)?;
    let connection = find_connection(&state.inner.root, &id).map_err(|error| error.message)?;
    let inner = state.inner.clone();
    serialized(inner, move |root| restore(root, &connection, &backup_id)).await
}

#[tauri::command]
fn get_data_directory(state: State<'_, AppState>) -> String {
    state.inner.root.display().to_string()
}

#[tauri::command]
fn open_data_directory(state: State<'_, AppState>) -> Result<(), String> {
    let path = state.inner.root.clone();
    #[cfg(target_os = "macos")]
    let result = Command::new("open").arg(&path).spawn();
    #[cfg(target_os = "windows")]
    let result = Command::new("explorer.exe").arg(&path).spawn();
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    let result = Command::new("xdg-open").arg(&path).spawn();
    result.map(|_| ()).map_err(|error| format!("Не удалось открыть папку: {error}"))
}

fn initialize_data(root: &FsPath, legacy_sources: &[PathBuf]) -> Result<(), String> {
    eprintln!("PG Snapshots: data directory {}", root.display());
    create_private_dir(root).map_err(|error| error.to_string())?;
    create_private_dir(&root.join("backups")).map_err(|error| error.to_string())?;

    eprintln!("PG Snapshots: existing local config {}", root.join(CONFIG_FILE).exists());
    if !root.join(CONFIG_FILE).exists() {
        for source in legacy_sources {
            let config = source.join(CONFIG_FILE);
            eprintln!("PG Snapshots: legacy config candidate {} exists={}", config.display(), config.is_file());
            if !config.is_file() || source == root {
                continue;
            }
            let bytes = fs::read(&config).map_err(|error| format!("Не удалось перенести настройки: {error}"))?;
            let connections: Vec<Connection> = serde_json::from_slice(&bytes)
                .map_err(|error| format!("Не удалось прочитать старый список подключений: {error}"))?;
            save_connections(root, &connections).map_err(|error| error.message)?;
            break;
        }
    }
    eprintln!("PG Snapshots: local config after migration {}", root.join(CONFIG_FILE).exists());

    for source in legacy_sources {
        if source != root {
            copy_legacy_backups(&source.join("backups"), &root.join("backups"))?;
        }
    }
    Ok(())
}

fn copy_legacy_backups(source: &FsPath, destination: &FsPath) -> Result<(), String> {
    let entries = match fs::read_dir(source) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("Не удалось перенести архивы: {error}")),
    };
    for entry in entries {
        let entry = entry.map_err(|error| format!("Не удалось прочитать папку архивов: {error}"))?;
        let file_type = entry.file_type().map_err(|error| error.to_string())?;
        let id = entry.file_name().to_string_lossy().to_string();
        if !file_type.is_dir() || validate_connection_id(&id).is_err() {
            continue;
        }
        let destination_dir = destination.join(&id);
        create_private_dir(&destination_dir).map_err(|error| error.to_string())?;
        let backup_entries = fs::read_dir(entry.path()).map_err(|error| error.to_string())?;
        for backup_entry in backup_entries {
            let backup_entry = backup_entry.map_err(|error| error.to_string())?;
            let file_type = backup_entry.file_type().map_err(|error| error.to_string())?;
            let path = backup_entry.path();
            let extension = path.extension().and_then(|value| value.to_str());
            let backup_id = path.file_stem().and_then(|value| value.to_str()).unwrap_or_default();
            if !file_type.is_file()
                || !matches!(extension, Some("dump" | "json"))
                || validate_backup_id(backup_id).is_err()
            {
                continue;
            }
            let target = destination_dir.join(path.file_name().unwrap_or_default());
            if target.exists() {
                continue;
            }
            let mut input = fs::File::open(&path).map_err(|error| error.to_string())?;
            let mut output = create_private_file(&target).map_err(|error| error.to_string())?;
            std::io::copy(&mut input, &mut output).map_err(|error| error.to_string())?;
            output.sync_all().map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn load_connections(root: &FsPath) -> Result<Vec<Connection>, ApiError> {
    let path = root.join(CONFIG_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read(&path)
        .map_err(|error| ApiError::internal(format!("Не удалось прочитать connections.json: {error}")))?;
    serde_json::from_slice(&content)
        .map_err(|error| ApiError::internal(format!("Файл connections.json повреждён: {error}")))
}

fn save_connections(root: &FsPath, connections: &[Connection]) -> Result<(), ApiError> {
    let path = root.join(CONFIG_FILE);
    let temp = root.join("connections.json.tmp");
    let bytes = serde_json::to_vec_pretty(connections)
        .map_err(|error| ApiError::internal(format!("Не удалось сохранить подключения: {error}")))?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .map_err(|error| ApiError::internal(format!("Не удалось записать конфигурацию: {error}")))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| ApiError::internal(format!("Не удалось записать конфигурацию: {error}")))?;
    fs::rename(&temp, &path)
        .map_err(|error| ApiError::internal(format!("Не удалось сохранить конфигурацию: {error}")))?;
    Ok(())
}

fn find_connection(root: &FsPath, id: &str) -> Result<Connection, ApiError> {
    validate_connection_id(id)?;
    load_connections(root)?
        .into_iter()
        .find(|connection| connection.id == id)
        .ok_or_else(|| ApiError::not_found("Подключение не найдено."))
}

fn validate_connection_id(id: &str) -> Result<(), ApiError> {
    if id.len() == 16 && id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::bad("Некорректный идентификатор подключения."))
    }
}

fn validate_backup_id(id: &str) -> Result<(), ApiError> {
    if !id.is_empty() && id.len() <= 24 && id.bytes().all(|byte| byte.is_ascii_digit()) {
        Ok(())
    } else {
        Err(ApiError::bad("Некорректный идентификатор снимка."))
    }
}

fn validate_uri(uri: &str) -> Result<(), ApiError> {
    if uri.len() > 2048 || uri.chars().any(char::is_control) {
        return Err(ApiError::bad("Некорректная строка подключения PostgreSQL."));
    }
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| ApiError::bad("Используйте URI вида postgresql://user:password@host:5432/database."))?;
    if scheme != "postgres" && scheme != "postgresql" {
        return Err(ApiError::bad(
            "Поддерживаются URI со схемой postgres:// или postgresql://.",
        ));
    }
    let (authority, path) = rest
        .split_once('/')
        .ok_or_else(|| ApiError::bad("В строке подключения укажите имя базы после адреса сервера."))?;
    if authority.is_empty() || path.split('?').next().unwrap_or_default().is_empty() {
        return Err(ApiError::bad("Укажите сервер и имя базы в строке подключения."));
    }
    Ok(())
}

fn stable_id(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn database_status(uri: &str) -> Result<DatabaseStatus, ApiError> {
    let query = "SELECT json_build_object('database', current_database(), 'user', current_user, 'version', current_setting('server_version'), 'size', pg_size_pretty(pg_database_size(current_database())))::text";
    let output = psql(uri, query)?;
    let mut status: DatabaseStatus = serde_json::from_str(output.trim())
        .map_err(|error| ApiError::internal(format!("Не удалось разобрать ответ PostgreSQL: {error}")))?;
    (status.host, status.port) = database_endpoint(uri);
    Ok(status)
}

fn database_endpoint(uri: &str) -> (String, String) {
    let Some((_, rest)) = uri.split_once("://") else {
        return ("—".to_string(), "5432".to_string());
    };
    let Some((authority, tail)) = rest.split_once('/') else {
        return ("—".to_string(), "5432".to_string());
    };
    let host_port = authority.rsplit_once('@').map(|(_, value)| value).unwrap_or(authority);
    let (mut host, mut port) = if let Some(rest) = host_port.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            (host.to_string(), tail.strip_prefix(':').unwrap_or("5432").to_string())
        } else {
            (host_port.to_string(), "5432".to_string())
        }
    } else if let Some((host, port)) = host_port.rsplit_once(':') {
        (host.to_string(), port.to_string())
    } else {
        (host_port.to_string(), "5432".to_string())
    };
    if let Some((_, query)) = tail.split_once('?') {
        for field in query.split('&') {
            if let Some((key, value)) = field.split_once('=') {
                if key == "host" && !value.is_empty() {
                    host = value.to_string();
                }
                if key == "port" && !value.is_empty() {
                    port = value.to_string();
                }
            }
        }
    }
    (host, port)
}

fn psql(uri: &str, query: &str) -> Result<String, ApiError> {
    run_tool(
        "psql",
        &[
            "-X".into(),
            "-w".into(),
            "-qAt".into(),
            "-d".into(),
            "$DB".into(),
            "-c".into(),
            query.into(),
        ],
        uri,
    )
}

fn run_tool(program: &str, args: &[String], uri: &str) -> Result<String, ApiError> {
    let prepared = prepare_uri(uri)?;
    let executable = find_postgres_tool(program).ok_or_else(|| ApiError::internal(
        format!("{program} не найден. Установите клиентские утилиты PostgreSQL и добавьте их в PATH."),
    ))?;
    let mut command = Command::new(executable);
    let mut actual_args: Vec<OsString> = Vec::with_capacity(args.len());
    for arg in args {
        actual_args.push(if arg == "$DB" {
            OsString::from(prepared.uri.as_str())
        } else {
            OsString::from(arg.as_str())
        });
    }
    command.args(actual_args).env("PGCONNECT_TIMEOUT", "5");
    if let Some(password) = &prepared.password {
        command.env("PGPASSWORD", password);
    }
    let output = command.output().map_err(|error| {
        let guidance = if error.kind() == std::io::ErrorKind::NotFound {
            format!("{program} не найден. Установите клиентские утилиты PostgreSQL и добавьте их в PATH.")
        } else {
            format!("Не удалось запустить {program}: {error}")
        };
        ApiError::internal(guidance)
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let mut safe = stderr.replace(uri, "[строка подключения]");
        safe = safe.replace(&prepared.uri, "[строка подключения]");
        if let Some(password) = &prepared.password {
            if !password.is_empty() {
                safe = safe.replace(password, "[пароль]");
            }
        }
        let detail = if safe.is_empty() {
            "команда завершилась с ошибкой без пояснения".to_string()
        } else {
            safe
        };
        return Err(ApiError::internal(format!("{program}: {detail}")));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn find_postgres_tool(program: &str) -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    let filename = format!("{program}.exe");
    #[cfg(not(target_os = "windows"))]
    let filename = program.to_string();

    for directory in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let candidate = directory.join(&filename);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    #[cfg(target_os = "macos")]
    for directory in ["/opt/homebrew/opt/libpq/bin", "/usr/local/opt/libpq/bin"] {
        let candidate = PathBuf::from(directory).join(&filename);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    #[cfg(target_os = "windows")]
    {
        for variable in ["PROGRAMFILES", "PROGRAMFILES(X86)"] {
            let Some(program_files) = std::env::var_os(variable) else { continue };
            let installations = PathBuf::from(program_files).join("PostgreSQL");
            if let Ok(versions) = fs::read_dir(installations) {
                for version in versions.flatten() {
                    let candidate = version.path().join("bin").join(&filename);
                    if candidate.is_file() {
                        return Some(candidate);
                    }
                }
            }
        }
    }
    None
}

struct PreparedUri {
    uri: String,
    password: Option<String>,
}

fn prepare_uri(uri: &str) -> Result<PreparedUri, ApiError> {
    validate_uri(uri)?;
    let (scheme, rest) = uri.split_once("://").expect("validated URI");
    let (authority, tail) = rest.split_once('/').expect("validated URI");
    let (authority_without_password, mut password) = if let Some((userinfo, host)) = authority.rsplit_once('@') {
        if let Some((username, encoded_password)) = userinfo.split_once(':') {
            (
                format!("{username}@{host}"),
                Some(percent_decode(encoded_password, false)?),
            )
        } else {
            (authority.to_string(), None)
        }
    } else {
        (authority.to_string(), None)
    };
    let (path, query) = tail.split_once('?').unwrap_or((tail, ""));
    let mut kept_query = Vec::new();
    for item in query.split('&').filter(|item| !item.is_empty()) {
        let (key, value) = item.split_once('=').unwrap_or((item, ""));
        if key == "password" {
            if password.is_none() {
                password = Some(percent_decode(value, true)?);
            }
        } else {
            kept_query.push(item);
        }
    }
    let suffix = if kept_query.is_empty() {
        String::new()
    } else {
        format!("?{}", kept_query.join("&"))
    };
    Ok(PreparedUri {
        uri: format!("{scheme}://{authority_without_password}/{path}{suffix}"),
        password,
    })
}

fn database_uri(uri: &str, database: &str) -> Result<String, ApiError> {
    let (scheme, rest) = uri.split_once("://").expect("validated URI");
    let (authority, tail) = rest.split_once('/').expect("validated URI");
    let (path, query) = tail.split_once('?').unwrap_or((tail, ""));
    let _ = path;
    let mut kept_query = Vec::new();
    for item in query.split('&').filter(|item| !item.is_empty()) {
        let (key, _) = item.split_once('=').unwrap_or((item, ""));
        if key != "dbname" && key != "password" {
            kept_query.push(item);
        }
    }
    let suffix = if kept_query.is_empty() {
        String::new()
    } else {
        format!("?{}", kept_query.join("&"))
    };
    Ok(format!(
        "{scheme}://{authority}/{}{suffix}",
        percent_encode_path(database)
    ))
}

fn percent_encode_path(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            result.push(char::from(byte));
        } else {
            result.push_str(&format!("%{byte:02X}"));
        }
    }
    result
}

fn percent_decode(value: &str, plus_is_space: bool) -> Result<String, ApiError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let high = hex(bytes[index + 1])
                    .ok_or_else(|| ApiError::bad("Некорректное percent-кодирование URI."))?;
                let low = hex(bytes[index + 2])
                    .ok_or_else(|| ApiError::bad("Некорректное percent-кодирование URI."))?;
                decoded.push(high * 16 + low);
                index += 3;
            }
            b'%' => return Err(ApiError::bad("Некорректное percent-кодирование URI.")),
            b'+' if plus_is_space => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| ApiError::bad("Строка подключения содержит некорректный UTF-8."))
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn backup_directory(root: &FsPath, connection_id: &str) -> PathBuf {
    root.join("backups").join(connection_id)
}

fn create_private_dir(path: &FsPath) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn create_private_file(path: &FsPath) -> std::io::Result<fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn backup_paths(root: &FsPath, connection_id: &str, backup_id: &str) -> (PathBuf, PathBuf) {
    let directory = backup_directory(root, connection_id);
    (
        directory.join(format!("{backup_id}.dump")),
        directory.join(format!("{backup_id}.json")),
    )
}

fn make_backup(root: &FsPath, connection: &Connection, name: &str) -> Result<Backup, ApiError> {
    let directory = backup_directory(root, &connection.id);
    create_private_dir(&directory)
        .map_err(|error| ApiError::internal(format!("Не удалось создать папку backup: {error}")))?;
    let id = now_nanos().to_string();
    let timestamp = now_millis();
    let final_dump = directory.join(format!("{id}.dump"));
    let temp_dump = directory.join(format!("{id}.dump.part"));
    let private_dump = create_private_file(&temp_dump)
        .map_err(|error| ApiError::internal(format!("Не удалось подготовить файл снимка: {error}")))?;
    drop(private_dump);
    let result = run_tool(
        "pg_dump",
        &[
            "--no-password".into(),
            "--format=custom".into(),
            format!("--file={}", temp_dump.display()),
            "--dbname".into(),
            "$DB".into(),
        ],
        &connection.uri,
    );
    if let Err(error) = result {
        let _ = fs::remove_file(&temp_dump);
        return Err(error);
    }
    let bytes = fs::metadata(&temp_dump)
        .map_err(|error| ApiError::internal(format!("Не удалось прочитать созданный снимок: {error}")))?
        .len();
    fs::rename(&temp_dump, &final_dump)
        .map_err(|error| ApiError::internal(format!("Не удалось сохранить снимок: {error}")))?;
    let meta = BackupMeta {
        id: id.clone(),
        name: name.to_string(),
        created_at: timestamp,
    };
    let meta_path = directory.join(format!("{id}.json"));
    let temp_meta = directory.join(format!("{id}.json.part"));
    let meta_bytes = serde_json::to_vec(&meta)
        .map_err(|error| ApiError::internal(format!("Не удалось подготовить описание снимка: {error}")))?;
    let meta_write = create_private_file(&temp_meta).and_then(|mut file| {
        file.write_all(&meta_bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_meta, &meta_path)
    });
    if let Err(error) = meta_write {
        let _ = fs::remove_file(&final_dump);
        let _ = fs::remove_file(&temp_meta);
        return Err(ApiError::internal(format!("Не удалось сохранить описание снимка: {error}")));
    }
    Ok(Backup {
        id,
        name: meta.name,
        created_at: meta.created_at,
        bytes,
    })
}

fn read_backups(root: &FsPath, connection_id: &str) -> Result<Vec<Backup>, ApiError> {
    let directory = backup_directory(root, connection_id);
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let entries = fs::read_dir(&directory)
        .map_err(|error| ApiError::internal(format!("Не удалось прочитать папку backup: {error}")))?;
    let mut backups = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| ApiError::internal(format!("Ошибка чтения папки backup: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let meta: BackupMeta = match serde_json::from_slice(&bytes) {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if validate_backup_id(&meta.id).is_err() {
            continue;
        }
        let (dump, _) = backup_paths(root, connection_id, &meta.id);
        let file_bytes = match fs::metadata(dump) {
            Ok(metadata) => metadata.len(),
            Err(_) => continue,
        };
        backups.push(Backup {
            id: meta.id,
            name: meta.name,
            created_at: meta.created_at,
            bytes: file_bytes,
        });
    }
    backups.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    Ok(backups)
}

fn remove_backup(
    root: &FsPath,
    connection_id: &str,
    backup_id: &str,
) -> Result<(), ApiError> {
    let (dump, meta) = backup_paths(root, connection_id, backup_id);
    if !dump.exists() {
        return Err(ApiError::not_found("Снимок не найден."));
    }
    fs::remove_file(&dump)
        .map_err(|error| ApiError::internal(format!("Не удалось удалить снимок: {error}")))?;
    let _ = fs::remove_file(meta);
    Ok(())
}

fn restore(root: &FsPath, connection: &Connection, backup_id: &str) -> Result<String, ApiError> {
    let (archive, _) = backup_paths(root, &connection.id, backup_id);
    if !archive.is_file() {
        return Err(ApiError::not_found("Файл снимка не найден."));
    }

    let info = database_info(&connection.uri)?;
    let database = info.database.as_str();
    if database == "template0" || database == "template1" {
        return Err(ApiError::bad(
            "Нельзя восстановить снимок поверх системного шаблона PostgreSQL.",
        ));
    }
    let stamp = now_nanos();
    let staging_db = format!("__pgsnap_new_{stamp}");
    let retired_db = format!("__pgsnap_old_{stamp}");
    let maintenance_uri = maintenance_uri(&connection.uri, database)?;
    create_empty_database(&maintenance_uri, &staging_db, &info)?;

    let staging_uri = database_uri(&connection.uri, &staging_db)?;
    let restore_result = restore_archive_with_compatibility(root, &archive, &staging_uri);
    if let Err(error) = restore_result {
        let _ = psql(
            &maintenance_uri,
            &format!("DROP DATABASE IF EXISTS {}", quote_identifier(&staging_db)),
        );
        return Err(error);
    }

    let warning = switch_database(&maintenance_uri, database, &staging_db, &retired_db)?;
    Ok(match warning {
        Some(warning) => format!(
            "База {database} восстановлена из снимка. Старая временная база {retired_db} осталась: {warning}"
        ),
        None => format!(
            "База {database} восстановлена из снимка. Все прежние подключения к ней были завершены."
        ),
    })
}

fn restore_archive_with_compatibility(
    root: &FsPath,
    archive: &FsPath,
    database_uri: &str,
) -> Result<(), ApiError> {
    let args = [
        "--no-password".into(),
        "--exit-on-error".into(),
        "--no-owner".into(),
        "--single-transaction".into(),
        "--dbname".into(),
        "$DB".into(),
        archive.to_string_lossy().into_owned(),
    ];

    match run_tool("pg_restore", &args, database_uri) {
        Ok(_) => Ok(()),
        Err(error) if is_unsupported_transaction_timeout(&error.message) => {
            restore_archive_without_transaction_timeout(root, archive, database_uri)
        }
        Err(error) => Err(error),
    }
}

fn is_unsupported_transaction_timeout(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("unrecognized configuration parameter \"transaction_timeout\"")
        && message.contains("set transaction_timeout = 0")
}

fn restore_archive_without_transaction_timeout(
    root: &FsPath,
    archive: &FsPath,
    database_uri: &str,
) -> Result<(), ApiError> {
    let script = root.join(format!(".restore-{}.sql", now_nanos()));
    let private_file = create_private_file(&script).map_err(|error| {
        ApiError::internal(format!("Не удалось подготовить совместимое восстановление: {error}"))
    })?;
    drop(private_file);

    let generate_result = run_tool(
        "pg_restore",
        &[
            "--no-password".into(),
            "--exit-on-error".into(),
            "--no-owner".into(),
            "--single-transaction".into(),
            "--file".into(),
            script.to_string_lossy().into_owned(),
            archive.to_string_lossy().into_owned(),
        ],
        database_uri,
    );
    if let Err(error) = generate_result {
        let _ = fs::remove_file(&script);
        return Err(error);
    }

    let restore_result = run_filtered_sql_script(&script, database_uri);
    let _ = fs::remove_file(&script);
    restore_result
}

fn run_filtered_sql_script(script: &FsPath, database_uri: &str) -> Result<(), ApiError> {
    let prepared = prepare_uri(database_uri)?;
    let executable = find_postgres_tool("psql").ok_or_else(|| {
        ApiError::internal("psql не найден. Установите клиентские утилиты PostgreSQL и добавьте их в PATH.")
    })?;
    let mut command = Command::new(executable);
    command
        .args(["-X", "-w", "-q", "-v", "ON_ERROR_STOP=1", "--dbname"])
        .arg(&prepared.uri)
        .env("PGCONNECT_TIMEOUT", "5")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(password) = &prepared.password {
        command.env("PGPASSWORD", password);
    }

    let mut child = command
        .spawn()
        .map_err(|error| ApiError::internal(format!("Не удалось запустить psql: {error}")))?;
    let stdin = child.stdin.take().ok_or_else(|| {
        ApiError::internal("Не удалось передать SQL-скрипт в psql.")
    })?;
    let script = script.to_path_buf();
    let writer = thread::spawn(move || copy_script_without_transaction_timeout(&script, stdin));
    let output = child
        .wait_with_output()
        .map_err(|error| ApiError::internal(format!("Не удалось дождаться завершения psql: {error}")))?;
    let write_result = writer
        .join()
        .map_err(|_| ApiError::internal("Передача SQL-скрипта в psql завершилась аварийно."))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let mut safe = stderr.replace(database_uri, "[строка подключения]");
        safe = safe.replace(&prepared.uri, "[строка подключения]");
        if let Some(password) = &prepared.password {
            if !password.is_empty() {
                safe = safe.replace(password, "[пароль]");
            }
        }
        let detail = if safe.is_empty() {
            "команда завершилась с ошибкой без пояснения".to_string()
        } else {
            safe
        };
        return Err(ApiError::internal(format!("psql: {detail}")));
    }

    write_result.map_err(|error| {
        ApiError::internal(format!("Не удалось передать SQL-скрипт в psql: {error}"))
    })
}

fn copy_script_without_transaction_timeout(
    script: &FsPath,
    mut destination: impl Write,
) -> std::io::Result<()> {
    let file = fs::File::open(script)?;
    let mut source = BufReader::new(file);
    let mut line = Vec::new();
    let mut removed_timeout_setting = false;
    loop {
        line.clear();
        if source.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if !removed_timeout_setting && is_transaction_timeout_statement(&line) {
            removed_timeout_setting = true;
            continue;
        }
        destination.write_all(&line)?;
    }
    destination.flush()
}

fn is_transaction_timeout_statement(line: &[u8]) -> bool {
    let mut statement = line
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|part| !part.is_empty());
    let Some(set) = statement.next() else {
        return false;
    };
    let Some(parameter) = statement.next() else {
        return false;
    };
    let Some(equals) = statement.next() else {
        return false;
    };
    let Some(value) = statement.next() else {
        return false;
    };
    let value = value.strip_suffix(b";").unwrap_or(value);
    set.eq_ignore_ascii_case(b"SET")
        && parameter.eq_ignore_ascii_case(b"transaction_timeout")
        && equals == b"="
        && value == b"0"
        && statement.next().is_none()
}

#[derive(Deserialize)]
struct DatabaseInfo {
    database: String,
    encoding: String,
    collate: String,
    ctype: String,
}

fn database_info(uri: &str) -> Result<DatabaseInfo, ApiError> {
    let output = psql(
        uri,
        "SELECT json_build_object('database', current_database(), 'encoding', pg_encoding_to_char(encoding), 'collate', datcollate, 'ctype', datctype)::text FROM pg_database WHERE datname = current_database()",
    )?;
    serde_json::from_str(&output)
        .map_err(|error| ApiError::internal(format!("Не удалось прочитать параметры базы: {error}")))
}

fn maintenance_uri(uri: &str, database: &str) -> Result<String, ApiError> {
    let maintenance_db = if database == "postgres" {
        "template1"
    } else {
        "postgres"
    };
    database_uri(uri, maintenance_db)
}

fn create_empty_database(
    maintenance_uri: &str,
    database_name: &str,
    info: &DatabaseInfo,
) -> Result<(), ApiError> {
    let create_sql = format!(
        "CREATE DATABASE {} WITH TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}",
        quote_identifier(database_name),
        quote_literal(&info.encoding),
        quote_literal(&info.collate),
        quote_literal(&info.ctype)
    );
    psql(maintenance_uri, &create_sql)?;
    Ok(())
}

fn switch_database(
    maintenance_uri: &str,
    database: &str,
    staging_db: &str,
    retired_db: &str,
) -> Result<Option<String>, ApiError> {
    let terminate_sql = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = {} AND pid <> pg_backend_pid()",
        quote_literal(database)
    );
    if let Err(error) = psql(maintenance_uri, &terminate_sql) {
        let _ = psql(
            maintenance_uri,
            &format!("DROP DATABASE IF EXISTS {}", quote_identifier(staging_db)),
        );
        return Err(error);
    }

    if let Err(error) = psql(
        maintenance_uri,
        &format!(
            "ALTER DATABASE {} RENAME TO {}",
            quote_identifier(database),
            quote_identifier(retired_db)
        ),
    ) {
        let _ = psql(
            maintenance_uri,
            &format!("DROP DATABASE IF EXISTS {}", quote_identifier(staging_db)),
        );
        return Err(error);
    }

    if let Err(error) = psql(
        maintenance_uri,
        &format!(
            "ALTER DATABASE {} RENAME TO {}",
            quote_identifier(staging_db),
            quote_identifier(database)
        ),
    ) {
        let rollback = psql(
            maintenance_uri,
            &format!(
                "ALTER DATABASE {} RENAME TO {}",
                quote_identifier(retired_db),
                quote_identifier(database)
            ),
        );
        let _ = psql(
            maintenance_uri,
            &format!("DROP DATABASE IF EXISTS {}", quote_identifier(staging_db)),
        );
        return Err(match rollback {
            Ok(_) => error,
            Err(rollback_error) => ApiError::internal(format!(
                "{}; также не удалось вернуть исходное имя базы: {}",
                error.message, rollback_error.message
            )),
        });
    }

    match psql(
        maintenance_uri,
        &format!("DROP DATABASE {}", quote_identifier(retired_db)),
    ) {
        Ok(_) => Ok(None),
        Err(error) => Ok(Some(error.message)),
    }
}

fn reset_database(root: &FsPath, connection: &Connection) -> Result<String, ApiError> {
    let info = database_info(&connection.uri)?;
    let database = info.database.as_str();
    if database == "template0" || database == "template1" {
        return Err(ApiError::bad(
            "Нельзя очистить системный шаблон PostgreSQL.",
        ));
    }

    let backup_name = "Автоснимок перед созданием пустой базы";
    let backup = make_backup(root, connection, backup_name)?;

    let stamp = now_nanos();
    let staging_db = format!("__pgsnap_empty_{stamp}");
    let retired_db = format!("__pgsnap_old_{stamp}");
    let maintenance_uri = maintenance_uri(&connection.uri, database)?;
    if let Err(error) = create_empty_database(&maintenance_uri, &staging_db, &info) {
        return Err(ApiError::internal(format!(
            "Автоснимок «{}» уже сохранён. Пустую базу создать не удалось: {}",
            backup.name, error.message
        )));
    }
    let warning = match switch_database(&maintenance_uri, database, &staging_db, &retired_db) {
        Ok(warning) => warning,
        Err(error) => {
            return Err(ApiError::internal(format!(
                "Автоснимок «{}» уже сохранён. Переключить базу не удалось: {}",
                backup.name, error.message
            )));
        }
    };

    Ok(match warning {
        Some(warning) => format!(
            "Создана пустая база {database}. Автоснимок «{backup_name}» сохранён. Старую временную базу {retired_db} не удалось удалить: {warning}"
        ),
        None => format!(
            "Создана пустая база {database}. Автоснимок «{backup_name}» сохранён в истории."
        ),
    })
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
