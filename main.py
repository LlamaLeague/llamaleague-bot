# main.py — LlamaLeague Bot v3
# Invita jugadores directamente en Dota 2 cuando confirman asistencia

import os, time, logging, threading
from dotenv import load_dotenv
from supabase import create_client
import steam.client
import dota2.client
from dota2.enums import DOTA_GameMode, DOTALobbyVisibility

load_dotenv('../.env.local')

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('bot')

sb = create_client(os.environ['NEXT_PUBLIC_SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])

SERVER_MAP = {
    'peru':      3,
    'chile':     3,
    'brazil':    14,
    'argentina': 3,
    'us_east':   1,
}
MODE_MAP = {'ap': 1, 'cm': 2, 'turbo': 23, 'ar': 5}

active_sala_id   = None
wo_timer         = None
jugadores_invitados = set()  # SteamIDs ya invitados en la sala actual

steam_client = steam.client.SteamClient()
dota_client  = dota2.client.Dota2Client(steam_client)

# ─── Login ────────────────────────────────────────────────────────────────────
@steam_client.on('logged_on')
def on_logged_on():
    log.info('Login Steam OK. Lanzando Dota 2...')
    dota_client.launch()

@steam_client.on('error')
def on_error(result):
    log.error(f'Error Steam: {result}')

@steam_client.on('disconnected')
def on_disconnected():
    global active_sala_id
    log.warning('Desconectado. Reconectando en 30s...')
    active_sala_id = None
    time.sleep(30)
    login()

@dota_client.on('ready')
def on_dota_ready():
    log.info('Dota 2 GC listo.')
    recuperar_sala_activa()
    start_polling()

# ─── Recuperar sala activa tras reinicio ──────────────────────────────────────
def recuperar_sala_activa():
    global active_sala_id, wo_timer
    res = sb.table('lobbies').select('*').eq('status', 'waiting').order('created_at').limit(1).execute()
    if not res.data: return
    sala = res.data[0]
    from datetime import datetime, timezone
    restantes = (datetime.fromisoformat(sala['wo_deadline'].replace('Z','+00:00')) - datetime.now(timezone.utc)).total_seconds()
    active_sala_id = sala['id']
    if restantes <= 0:
        handle_wo(sala)
    else:
        log.info(f"Retomando sala {sala['id']}. WO en {int(restantes)}s")
        wo_timer = threading.Timer(restantes, handle_wo, args=[sala])
        wo_timer.start()

# ─── Polling ──────────────────────────────────────────────────────────────────
def start_polling():
    threading.Thread(target=poll_queue,     daemon=True).start()
    threading.Thread(target=poll_wo,        daemon=True).start()
    threading.Thread(target=poll_heartbeat, daemon=True).start()
    threading.Thread(target=poll_invites,   daemon=True).start()
    log.info('Polling activo. Esperando salas...')

def poll_queue():
    while True:
        if not active_sala_id: check_queue()
        time.sleep(8)

def poll_wo():
    while True:
        check_wo_deadlines()
        time.sleep(15)

def poll_heartbeat():
    while True:
        try:
            res = sb.table('lobbies').select('*', count='exact').in_('status', ['waiting','active']).execute()
            log.info(f'Heartbeat | sala: {active_sala_id or "ninguna"} | DB: {res.count or 0}')
        except Exception as e:
            log.error(f'Heartbeat: {e}')
        time.sleep(60)

# ─── Poll invitaciones ────────────────────────────────────────────────────────
# Cada 5 segundos revisa jugadores confirmados y les envía invitación en Dota 2
def poll_invites():
    global jugadores_invitados
    while True:
        if active_sala_id:
            try:
                # Obtener jugadores confirmados con su steam_id
                res = sb.table('lobby_players').select(
                    'user_id, users(steam_id)'
                ).eq('lobby_id', active_sala_id).eq('confirmed', True).execute()

                for p in (res.data or []):
                    steam_id_str = p.get('users', {}).get('steam_id')
                    if not steam_id_str:
                        continue
                    steam_id = int(steam_id_str)
                    if steam_id not in jugadores_invitados:
                        try:
                            dota_client.invite_to_lobby(steam_id)
                            jugadores_invitados.add(steam_id)
                            log.info(f'✓ Invitación enviada a SteamID: {steam_id}')
                        except Exception as e:
                            log.error(f'invite_to_lobby {steam_id}: {e}')
            except Exception as e:
                log.error(f'poll_invites: {e}')
        else:
            # Limpiar cuando no hay sala activa
            if jugadores_invitados:
                jugadores_invitados = set()
        time.sleep(5)

# ─── Queue ────────────────────────────────────────────────────────────────────
def check_queue():
    try:
        res = sb.table('lobbies').select('*').eq('status', 'queued').order('created_at').limit(1).execute()
        if res.data:
            log.info(f"Sala en cola: {res.data[0]['id']}")
            procesar_sala(res.data[0])
    except Exception as e:
        log.error(f'check_queue: {e}')

def procesar_sala(sala):
    global active_sala_id, wo_timer, jugadores_invitados
    active_sala_id = sala['id']
    jugadores_invitados = set()  # reset para nueva sala

    from datetime import datetime, timezone, timedelta
    wo_deadline = (datetime.now(timezone.utc) + timedelta(minutes=sala['wo_timer'])).isoformat()
    sb.table('lobbies').update({'status': 'waiting', 'wo_deadline': wo_deadline}).eq('id', sala['id']).execute()

    try:
        crear_lobby_dota2(sala)
    except Exception as e:
        log.error(f'Error lobby: {e}')
        sb.table('lobbies').update({'status': 'queued'}).eq('id', sala['id']).execute()
        active_sala_id = None
        return

    wo_timer = threading.Timer(sala['wo_timer'] * 60, handle_wo, args=[sala])
    wo_timer.start()
    log.info(f"✓ Lobby listo. Pass: {sala['password']} | WO: {sala['wo_timer']}min")

# ─── Crear lobby ──────────────────────────────────────────────────────────────
def crear_lobby_dota2(sala):
    evento = threading.Event()

    def on_lobby_new(lobby):
        log.info(f"Lobby ID: {lobby.lobby_id}")
        # Mover bot a espectador
        try:
            dota_client.join_practice_lobby_team(4)
            log.info('Bot → espectador OK')
        except Exception as e:
            log.warning(f'join_team(4): {e}')
            try:
                dota_client.join_practice_lobby_broadcast_channel(0)
                log.info('Bot → broadcast OK')
            except Exception as e2:
                log.warning(f'broadcast(0): {e2}')
        evento.set()

    dota_client.once('lobby_new', on_lobby_new)
    dota_client.create_practice_lobby(
        password=sala['password'],
        options={
            'game_name':        f"LlamaLeague",
            'server_region':    SERVER_MAP.get(sala['server'], 3),
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

# ─── Eventos lobby ────────────────────────────────────────────────────────────
@dota_client.on('lobby_changed')
def on_lobby_changed(lobby):
    if not active_sala_id: return
    try:
        members = lobby.members or []
        jugadores = [m for m in members if getattr(m, 'team', -1) in (0, 1)]
        count = len(jugadores)

        log.info(f'Lobby: {count}/10 jugadores')
        sb.table('lobbies').update({'player_count': count}).eq('id', active_sala_id).execute()

        if count >= 10:
            if wo_timer: wo_timer.cancel()
            res = sb.table('lobbies').select('status').eq('id', active_sala_id).execute()
            if res.data and res.data[0]['status'] == 'waiting':
                iniciar_partida(active_sala_id)
    except Exception as e:
        log.error(f'lobby_changed: {e}')

# ─── WO ───────────────────────────────────────────────────────────────────────
def handle_wo(sala):
    global active_sala_id
    try:
        res = sb.table('lobbies').select('status').eq('id', sala['id']).execute()
        if not res.data or res.data[0]['status'] != 'waiting':
            active_sala_id = None
            return
        players_res = sb.table('lobby_players').select('user_id, team').eq('lobby_id', sala['id']).eq('confirmed', True).execute()
        players = players_res.data or []
        total = len(players)
        if total >= 10:   iniciar_partida(sala['id'])
        elif total == 0:  cancelar_sala(sala['id'])
        else:
            radiant = sum(1 for p in players if p['team'] == 'radiant')
            dire    = sum(1 for p in players if p['team'] == 'dire')
            reportar_resultado(sala['id'], 'radiant' if radiant >= dire else 'dire', sala['community_id'], players)
    except Exception as e:
        log.error(f'handle_wo: {e}')

def check_wo_deadlines():
    try:
        from datetime import datetime, timezone
        ahora = datetime.now(timezone.utc).isoformat()
        res = sb.table('lobbies').select('*').eq('status', 'waiting').lt('wo_deadline', ahora).execute()
        for sala in (res.data or []):
            if active_sala_id and active_sala_id != sala['id']: continue
            if not active_sala_id: globals()['active_sala_id'] = sala['id']
            if active_sala_id == sala['id']: handle_wo(sala)
    except Exception as e:
        log.error(f'check_wo_deadlines: {e}')

# ─── Acciones ─────────────────────────────────────────────────────────────────
def iniciar_partida(sala_id):
    global active_sala_id, wo_timer
    try: dota_client.launch_practice_lobby()
    except: pass
    sb.table('lobbies').update({'status': 'active', 'started_at': _now()}).eq('id', sala_id).execute()
    if wo_timer: wo_timer.cancel()
    active_sala_id = None
    log.info(f'Partida iniciada: {sala_id}')

def cancelar_sala(sala_id):
    global active_sala_id, wo_timer
    try: dota_client.destroy_lobby()
    except: pass
    sb.table('lobbies').update({'status': 'cancelled', 'ended_at': _now()}).eq('id', sala_id).execute()
    if wo_timer: wo_timer.cancel()
    active_sala_id = None
    log.info(f'Sala cancelada: {sala_id}')

def reportar_resultado(sala_id, winner, community_id, players):
    global active_sala_id, wo_timer
    sb.table('lobbies').update({'status': 'completed', 'winner': winner, 'ended_at': _now()}).eq('id', sala_id).execute()
    for p in players:
        won, delta = p['team'] == winner, 35 if p['team'] == winner else -10
        ex_res = sb.table('ranking').select('id,points,wins,losses').eq('community_id', community_id).eq('user_id', p['user_id']).execute()
        ex = ex_res.data[0] if ex_res.data else None
        if ex:
            sb.table('ranking').update({
                'points': max(0, ex['points']+delta),
                'wins':   ex['wins']+(1 if won else 0),
                'losses': ex['losses']+(0 if won else 1)
            }).eq('id', ex['id']).execute()
        else:
            sb.table('ranking').insert({
                'community_id': community_id, 'user_id': p['user_id'],
                'points': max(0,1000+delta), 'wins': 1 if won else 0,
                'losses': 0 if won else 1, 'season': 1
            }).execute()
    all_res = sb.table('ranking').select('id,points').eq('community_id', community_id).order('points', desc=True).execute()
    for i, r in enumerate(all_res.data or []):
        sb.table('ranking').update({'position': i+1}).eq('id', r['id']).execute()
    if wo_timer: wo_timer.cancel()
    active_sala_id = None
    log.info(f'{winner} gana: {sala_id}')

def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

def login():
    log.info('Iniciando sesion...')
    steam_client.login(username=os.environ['BOT_STEAM_USER'], password=os.environ['BOT_STEAM_PASS'])

if __name__ == '__main__':
    login()
    steam_client.run_forever()
