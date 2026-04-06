# main.py — LlamaLeague Bot v5
# Python 3.11 | Render (Docker)
# Lógica: crea salas en paralelo, sin WO, reporta resultado y abona economía

import os, time, logging, threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import steam.client
import dota2.client
from dota2.enums import DOTA_GameMode, DOTALobbyVisibility

# Railway inyecta las vars directamente — load_dotenv como fallback local
load_dotenv('.env')
load_dotenv('../.env.local')  # solo para desarrollo local

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('bot')

# ── Validar variables de entorno ─────────────────────────────────────────────
REQUIRED_ENV = ['NEXT_PUBLIC_SUPABASE_URL', 'SUPABASE_SERVICE_KEY', 'BOT_STEAM_USER', 'BOT_STEAM_PASS']
for k in REQUIRED_ENV:
    if not os.environ.get(k):
        log.error(f'ERROR: falta variable de entorno: {k}')
        exit(1)

# ── Supabase ──────────────────────────────────────────────────────────────────
sb = create_client(
    os.environ['NEXT_PUBLIC_SUPABASE_URL'],
    os.environ['SUPABASE_SERVICE_KEY']
)

# ── Mapas Dota 2 ───────────────────────────────────────────────────────────────
SERVER_MAP = {
    'peru':      21,  # Peru server propio
    'chile':     20,  # Chile server propio
    'brazil':    12,  # Brazil
    'argentina': 23,  # Argentina server propio
    'us_east':    2,  # US East
    'us_west':    1,  # US West
}
MODE_MAP = {
    'ap': 1, 'cm': 2, 'turbo': 23, 'ar': 5,
}

# ── Estado global ─────────────────────────────────────────────────────────────
# Dict de salas que el bot está manejando: sala_id → {'sala': {...}, 'invitados': set()}
salas_activas = {}
salas_lock    = threading.Lock()

steam_client = steam.client.SteamClient()
dota_client  = dota2.client.Dota2Client(steam_client)

# ── Login Steam ────────────────────────────────────────────────────────────────
@steam_client.on('logged_on')
def on_logged_on():
    log.info('Steam OK — lanzando Dota 2...')
    dota_client.launch()

@steam_client.on('error')
def on_error(result):
    log.error(f'Error Steam: {result}')

@steam_client.on('disconnected')
def on_disconnected():
    log.warning('Desconectado — reconectando en 30s...')
    time.sleep(30)
    login()

@dota_client.on('ready')
def on_dota_ready():
    log.info('Dota 2 GC listo.')
    sincronizar_salas_activas()
    start_polling()

# ── Al reconectar: recuperar salas en curso ────────────────────────────────────
def sincronizar_salas_activas():
    # Destruir cualquier lobby viejo que el bot tenga abierto en Dota 2
    try:
        dota_client.destroy_lobby()
        log.info('Lobby anterior destruido al arrancar.')
    except:
        pass

    # Limpiar salas waiting con started_at != null que quedaron colgadas
    # (bot se reinició mientras procesaba) — resetear para reintento
    res0 = sb.table('lobbies').select('id') \
        .eq('status', 'waiting').not_.is_('started_at', 'null').execute()
    for sala in (res0.data or []):
        sb.table('lobbies').update({'started_at': None}).eq('id', sala['id']).execute()
        log.info(f"Sala colgada reseteada: {sala['id'][:8]}")

    # Salas waiting sin started_at = pendientes de crear lobby
    res = sb.table('lobbies').select('*') \
        .eq('status', 'waiting').is_('started_at', 'null').execute()
    for sala in (res.data or []):
        log.info(f"Sala pendiente al reiniciar: {sala['id'][:8]}")
        threading.Thread(target=procesar_sala, args=[sala], daemon=True).start()

    # Salas active = ya en partida, solo registrar en memoria
    res2 = sb.table('lobbies').select('id').eq('status', 'active').execute()
    for sala in (res2.data or []):
        with salas_lock:
            salas_activas[sala['id']] = {'sala': sala, 'invitados': set()}
        log.info(f"Sala activa registrada: {sala['id'][:8]}")

# ── Polling ────────────────────────────────────────────────────────────────────
def start_polling():
    threading.Thread(target=poll_nuevas_salas,    daemon=True).start()
    threading.Thread(target=poll_invites,          daemon=True).start()
    threading.Thread(target=poll_heartbeat,        daemon=True).start()
    threading.Thread(target=poll_cancelaciones,    daemon=True).start()
    log.info('Polling iniciado — esperando salas...')

def poll_nuevas_salas():
    """Detecta salas waiting con started_at=null y las procesa"""
    while True:
        try:
            res = sb.table('lobbies').select('*') \
                .eq('status', 'waiting').is_('started_at', 'null') \
                .order('created_at').execute()
            for sala in (res.data or []):
                with salas_lock:
                    ya_en_curso = sala['id'] in salas_activas
                if not ya_en_curso:
                    log.info(f"Nueva sala detectada: {sala['id'][:8]} | {sala['mode']} | {sala['server']}")
                    threading.Thread(target=procesar_sala, args=[sala], daemon=True).start()
        except Exception as e:
            log.error(f'poll_nuevas_salas: {e}')
        time.sleep(6)

def poll_invites():
    """Reintenta invitar a jugadores que aún no fueron invitados"""
    while True:
        try:
            with salas_lock:
                ids = list(salas_activas.keys())
            for sala_id in ids:
                with salas_lock:
                    data = salas_activas.get(sala_id)
                if not data: continue

                # Solo invitar en salas waiting
                check = sb.table('lobbies').select('status').eq('id', sala_id).execute()
                if not check.data or check.data[0]['status'] != 'waiting':
                    continue

                res = sb.table('lobby_players') \
                    .select('user_id, users(steam_id, display_name)') \
                    .eq('lobby_id', sala_id).eq('confirmed', True).execute()

                for p in (res.data or []):
                    steam_id_str = (p.get('users') or {}).get('steam_id')
                    if not steam_id_str: continue
                    steam_id = int(steam_id_str)
                    with salas_lock:
                        invitados = salas_activas.get(sala_id, {}).get('invitados', set())
                    if steam_id not in invitados:
                        try:
                            dota_client.invite_to_lobby(steam_id)
                            nombre = (p.get('users') or {}).get('display_name', str(steam_id))
                            log.info(f'Invitado: {nombre} → sala {sala_id[:8]}')
                            with salas_lock:
                                if sala_id in salas_activas:
                                    salas_activas[sala_id]['invitados'].add(steam_id)
                        except Exception as e:
                            log.warning(f'invite {steam_id}: {e}')
        except Exception as e:
            log.error(f'poll_invites: {e}')
        time.sleep(5)

def poll_heartbeat():
    while True:
        try:
            with salas_lock:
                n = len(salas_activas)
                ids = [k[:8] for k in salas_activas.keys()]
            log.info(f'♥ Salas activas: {n} | {ids}')
        except Exception as e:
            log.error(f'heartbeat: {e}')
        time.sleep(60)

def poll_cancelaciones():
    """Detecta salas que el streamer canceló desde el frontend y destruye el lobby"""
    while True:
        try:
            with salas_lock:
                ids = list(salas_activas.keys())
            for sala_id in ids:
                res = sb.table('lobbies').select('status').eq('id', sala_id).execute()
                if not res.data: continue
                status = res.data[0]['status']
                if status == 'cancelled':
                    log.info(f'Sala {sala_id[:8]} cancelada desde frontend — destruyendo lobby...')
                    try:
                        dota_client.destroy_lobby()
                        log.info('Lobby Dota2 destruido.')
                    except Exception as e:
                        log.warning(f'destroy_lobby: {e}')
                    with salas_lock:
                        salas_activas.pop(sala_id, None)
        except Exception as e:
            log.error(f'poll_cancelaciones: {e}')
        time.sleep(5)

# ── Procesar una sala ─────────────────────────────────────────────────────────
def procesar_sala(sala):
    sala_id = sala['id']

    # Lock atómico: marcar started_at solo si sigue en null
    # Esto evita que dos threads tomen la misma sala
    try:
        res = sb.table('lobbies') \
            .update({'started_at': _now()}) \
            .eq('id', sala_id).is_('started_at', 'null').execute()
        if not res.data:
            log.warning(f'Sala {sala_id[:8]} ya tomada.')
            return
    except Exception as e:
        log.error(f'Lock sala {sala_id[:8]}: {e}')
        return

    with salas_lock:
        salas_activas[sala_id] = {'sala': sala, 'invitados': set()}

    try:
        crear_lobby_dota2(sala)
        log.info(f"✓ Lobby creado | sala {sala_id[:8]} | pass: {sala['password']}")
    except Exception as e:
        log.error(f'Error creando lobby {sala_id[:8]}: {e}')
        # Liberar para reintento
        sb.table('lobbies').update({'started_at': None}).eq('id', sala_id).execute()
        with salas_lock:
            salas_activas.pop(sala_id, None)

# ── Crear lobby en Dota 2 ─────────────────────────────────────────────────────
def crear_lobby_dota2(sala):
    evento = threading.Event()

    def on_lobby_new(lobby):
        log.info(f"Lobby Dota2 ID: {lobby.lobby_id}")
        # Bot pasa a espectador para no ocupar slot de jugador
        try:
            dota_client.join_practice_lobby_team(4)
            log.info('Bot → espectador OK')
        except Exception as e:
            log.warning(f'join_team(4): {e}')
            try:
                dota_client.join_practice_lobby_broadcast_channel(0)
            except:
                pass
        # Guardar lobby_id en DB
        try:
            sb.table('lobbies') \
                .update({'dota_lobby_id': str(lobby.lobby_id)}) \
                .eq('id', sala['id']).execute()
        except:
            pass
        evento.set()

    dota_client.once('lobby_new', on_lobby_new)
    dota_client.create_practice_lobby(
        password=sala['password'],
        options={
            'game_name':        'LlamaLeague',
            'server_region':    SERVER_MAP.get(sala['server'], 21),  # default Peru
            'game_mode':        MODE_MAP.get(sala['mode'], 1),
            'allow_cheats':     False,
            'fill_with_bots':   False,
            'allow_spectating': True,
            'visibility':       DOTALobbyVisibility.Public,
        }
    )

    if not evento.wait(timeout=30):
        dota_client.remove_listener('lobby_new', on_lobby_new)
        raise TimeoutError('GC timeout 30s')

# ── Evento: cambios en el lobby ───────────────────────────────────────────────
@dota_client.on('lobby_changed')
def on_lobby_changed(lobby):
    # Identificar a qué sala corresponde este lobby
    lobby_id_str = str(lobby.lobby_id) if hasattr(lobby, 'lobby_id') else None
    sala_id = None

    with salas_lock:
        ids = list(salas_activas.keys())

    for sid in ids:
        check = sb.table('lobbies').select('status, dota_lobby_id') \
            .eq('id', sid).execute()
        if not check.data: continue
        row = check.data[0]
        if row.get('dota_lobby_id') == lobby_id_str or row['status'] == 'waiting':
            sala_id = sid
            break

    if not sala_id: return

    try:
        members = list(getattr(lobby, 'all_members', None) or
                       getattr(lobby, 'members', None) or [])
        # teams 0=radiant, 1=dire
        jugadores = [m for m in members if getattr(m, 'team', -1) in (0, 1)]
        count = len(jugadores)

        if count > 0:
            sb.table('lobbies').update({'player_count': count}).eq('id', sala_id).execute()
            log.info(f'Sala {sala_id[:8]}: {count}/10 jugadores')

            # Sincronizar jugadores en DB desde Dota 2
            for m in jugadores:
                steam_id_str = str(getattr(m, 'id', None) or '')
                if not steam_id_str: continue
                # Ignorar el bot mismo
                if steam_id_str == str(getattr(steam_client, 'steam_id', '')):
                    continue
                team = 'radiant' if getattr(m, 'team', -1) == 0 else 'dire'
                # Buscar usuario por steam_id
                user_res = sb.table('users').select('id') \
                    .eq('steam_id', steam_id_str).execute()
                if not user_res.data:
                    continue
                user_id = user_res.data[0]['id']
                # Ver si ya está en lobby_players
                existing = sb.table('lobby_players') \
                    .select('id') \
                    .eq('lobby_id', sala_id) \
                    .eq('user_id', user_id).execute()
                if existing.data:
                    # Solo actualizar equipo
                    sb.table('lobby_players') \
                        .update({'team': team, 'confirmed': True}) \
                        .eq('lobby_id', sala_id) \
                        .eq('user_id', user_id).execute()
                else:
                    # Insertar — entró directo por contraseña
                    try:
                        sb.table('lobby_players').insert({
                            'lobby_id':  sala_id,
                            'user_id':   user_id,
                            'team':      team,
                            'confirmed': True,
                        }).execute()
                        log.info(f'Jugador añadido desde Dota2: {user_id[:8]} → {team}')
                    except Exception as e:
                        log.warning(f'insert lobby_player: {e}')

        # 10 jugadores → iniciar automáticamente
        if count >= 10:
            check2 = sb.table('lobbies').select('status').eq('id', sala_id).execute()
            if check2.data and check2.data[0]['status'] == 'waiting':
                iniciar_partida(sala_id)

    except Exception as e:
        log.error(f'lobby_changed {sala_id[:8]}: {e}')

# ── Iniciar partida ───────────────────────────────────────────────────────────
def iniciar_partida(sala_id):
    try:
        dota_client.launch_practice_lobby()
    except:
        pass
    sb.table('lobbies').update({
        'status':     'active',
        'started_at': _now(),
    }).eq('id', sala_id).execute()
    log.info(f'✓ Partida iniciada: {sala_id[:8]}')
    # NO borrar de salas_activas — seguimos escuchando para el resultado

# ── Resultado automático desde Dota 2 GC ─────────────────────────────────────
@dota_client.on('match_result')
def on_match_result(result):
    """Evento cuando Dota 2 reporta el resultado de la partida"""
    try:
        winner = 'radiant' if result.good_guys_win else 'dire'
        log.info(f'Match terminó — {winner} gana')

        # Buscar la sala activa
        res = sb.table('lobbies').select('id') \
            .eq('status', 'active').limit(1).execute()
        if not res.data:
            log.warning('No se encontró sala activa para el resultado.')
            return

        sala_id = res.data[0]['id']
        players_res = sb.table('lobby_players') \
            .select('user_id, team') \
            .eq('lobby_id', sala_id).eq('confirmed', True).execute()

        reportar_resultado(sala_id, winner, players_res.data or [])
    except Exception as e:
        log.error(f'on_match_result: {e}')

# ── Reportar resultado y abonar economía ─────────────────────────────────────
def reportar_resultado(sala_id, winner, players):
    # 1. Cerrar la sala
    sb.table('lobbies').update({
        'status':      'completed',
        'winner_team': winner,   # columna real en DB
        'ended_at':    _now(),
    }).eq('id', sala_id).execute()

    with salas_lock:
        salas_activas.pop(sala_id, None)

    log.info(f'Sala {sala_id[:8]} completada — {winner} gana')

    # 2. Obtener lc_reward de la sala
    sala_res = sb.table('lobbies').select('lc_reward').eq('id', sala_id).execute()
    lc_reward = (sala_res.data[0].get('lc_reward') if sala_res.data else None) or 5

    # 3. Abonar a cada jugador
    for p in players:
        try:
            won          = p['team'] == winner
            points_delta = 35 if won else -10
            lc_delta     = lc_reward if won else 0

            # Registrar en match_history
            sb.table('match_history').insert({
                'lobby_id':     sala_id,
                'user_id':      p['user_id'],
                'team':         p.get('team') or 'unknown',
                'won':          won,
                'points_delta': points_delta,
                'lc_delta':     lc_delta,
            }).execute()

            # Obtener stats actuales
            user_res = sb.table('users') \
                .select('points, wins, losses, lc_balance') \
                .eq('id', p['user_id']).execute()
            if not user_res.data: continue
            u = user_res.data[0]

            new_points = max(0, (u['points'] or 0) + points_delta)
            new_tier   = get_tier(new_points)

            sb.table('users').update({
                'points':     new_points,
                'tier':       new_tier,
                'wins':       (u['wins']       or 0) + (1 if won else 0),
                'losses':     (u['losses']     or 0) + (0 if won else 1),
                'lc_balance': (u['lc_balance'] or 0) + lc_delta,
                'updated_at': _now(),
            }).eq('id', p['user_id']).execute()

            log.info(f"  {p['user_id'][:8]}: {'+'if won else ''}{points_delta}pts, +{lc_delta}LC")

        except Exception as e:
            log.error(f"reportar jugador {p.get('user_id','?')[:8]}: {e}")

    log.info(f'✓ Economía abonada — sala {sala_id[:8]}')

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_tier(pts):
    if pts >= 5000: return 'Apukuna'
    if pts >= 4000: return 'Hatun Kuraka'
    if pts >= 3000: return 'Inmortal'
    if pts >= 2500: return 'Qhapaq'
    if pts >= 2000: return 'Wiñay'
    if pts >= 1500: return 'Supay'
    if pts >= 1000: return 'Inti'
    if pts >= 700:  return 'Willka'
    if pts >= 500:  return 'Apu'
    if pts >= 300:  return 'Sinchi'
    if pts >= 150:  return 'Ayllu'
    if pts >= 50:   return 'Kawsay'
    return 'Wawa'

def _now():
    return datetime.now(timezone.utc).isoformat()

def login():
    log.info('Iniciando sesión Steam...')
    steam_client.login(
        username=os.environ['BOT_STEAM_USER'],
        password=os.environ['BOT_STEAM_PASS'],
    )

# ── Arranque ──────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    login()
    steam_client.run_forever()
