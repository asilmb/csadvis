$content = @'
import time
import logging
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

from config import settings
from ui.theme import THEME_CONFIG
from infra.redis_client import get_redis

# Базовая версия (fallback), если Redis пуст или недоступен
_VERSION = "1.2.0"
_VERSION_REDIS_KEY = "cs2:system:version"
_CACHE_TTL = 60  # Секунд кэширования в памяти процесса

logger = logging.getLogger(__name__)

# --- Локальный кэш версии ---
class _CachedVersion:
    value: str = _VERSION
    expire_at: float = 0

def get_dynamic_version() -> str:
    """Получает версию из Redis с кэшированием в памяти процесса."""
    now = time.time()
    
    if now < _CachedVersion.expire_at:
        return _CachedVersion.value

    try:
        r = get_redis()
        val = r.get(_VERSION_REDIS_KEY)
        if val:
            _CachedVersion.value = val.decode('utf-8') if isinstance(val, bytes) else str(val)
        else:
            _CachedVersion.value = _VERSION
    except Exception as e:
        logger.error(f"Ошибка при чтении версии из Redis: {e}")
        _CachedVersion.value = _VERSION
    
    _CachedVersion.expire_at = now + _CACHE_TTL
    return _CachedVersion.value

# --- Инициализация App ---
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY, dbc.icons.BOOTSTRAP],
    update_title=None,
    suppress_callback_exceptions=True
)

def serve_layout():
    """Функция генерации layout, вызываемая при каждом визите на страницу."""
    current_version = get_dynamic_version()
    
    return html.Div([
        dcc.Location(id='url', refresh=False),
        
        # Навигационная панель
        dbc.NavbarSimple(
            brand=f"CS2 Market Analytics v{current_version}",
            brand_href="/",
            color="primary",
            dark=True,
            className="mb-4",
            children=[
                dbc.NavItem(dbc.NavLink("Портфель", href="/")),
                dbc.NavItem(dbc.NavLink("Инвентарь", href="/inventory")),
                dbc.NavItem(dbc.NavLink("Маркет", href="/market")),
                dbc.NavItem(dbc.NavLink("Аналитика", href="/analytics")),
            ]
        ),
        
        # Основной контент
        dbc.Container(id='page-content', fluid=True),
        
        # Индикатор версии в футере (опционально)
        html.Footer(
            dbc.Container(
                html.Small(f"Версия системы: {current_version}", className="text-muted"),
                className="text-center mt-5 pb-3"
            )
        )
    ], style=THEME_CONFIG['container_style'])

# Назначаем функцию в layout для поддержки динамических обновлений
app.layout = serve_layout

# Экспортируем сервер для gunicorn
server = app.server
'@

$content | Out-File -FilePath "src/ui/app.py" -Encoding utf8
Write-Host "Файл src/ui/app.py успешно обновлен для поддержки динамической версии." -ForegroundColor Green