const savedUser = localStorage.getItem('listen-together-user') || '';
const userInput = document.querySelector('#user-name');
const welcome = document.querySelector('#welcome');
const roomView = document.querySelector('#room');
const formError = document.querySelector('#form-error');
const connectionLabel = document.querySelector('#connection-label');
const connectionPill = document.querySelector('.live-indicator');
const notice = document.querySelector('#room-notice');
const params = new URLSearchParams(location.search);
let socket;
let roomId = '';
let inviteToken = '';
let roomState;
let knownPeople = new Map();
let pendingAcks = new Map();
let seeking = false;
let clockOffsetMs = 0;
let clockSamples = [];
let clockSyncTimer;
let audioEnabled = false;
let youtubePlayer;
let youtubePlayerIsReady = false;
let youtubeApiReady = false;
let activeYoutubeId = '';
let requestedYoutubeId = '';
let youtubeTitle = '';
let playAttemptKey = '';
let activeSpotifyUri = '';
let activeDirectUrl = '';
let directPlayerKind = '';
let directPlayBlocked = false;

window.onYouTubeIframeAPIReady = () => {
  youtubeApiReady = true;
  syncMediaPlayer();
};

for (const player of [document.querySelector('#direct-audio'), document.querySelector('#direct-video')]) {
  player.addEventListener('error', () => {
    const status = document.querySelector('#direct-status');
    status.textContent = 'Could not load this media URL. Use a public, direct MP3/MP4 file URL, not a page that hosts a player.';
    status.classList.add('error');
    audioEnabled = false;
    document.querySelector('#enable-audio').classList.remove('hidden');
    document.querySelector('#enable-audio').disabled = false;
    document.querySelector('#enable-audio').textContent = 'Retry synchronized playback';
  });
}

const youtubeApiScript = document.createElement('script');
youtubeApiScript.src = 'https://www.youtube.com/iframe_api';
youtubeApiScript.onerror = () => setYoutubeStatus('YouTube player could not load. Check your internet connection.', true);
document.head.append(youtubeApiScript);

userInput.value = savedUser;
if (savedUser) setProfile(savedUser);
if (params.get('room')) {
  const inviteInput = document.querySelector('#invite-link');
  inviteInput.value = `${location.origin}/?room=${encodeURIComponent(params.get('room'))}&invite=${encodeURIComponent(params.get('invite') || '')}`;
}

function setProfile(name) {
  document.querySelector('#profile-name').textContent = name || 'Guest';
  document.querySelector('#avatar').textContent = (name || '?').trim().charAt(0).toUpperCase();
}

function userId() {
  const name = userInput.value.trim().slice(0, 32);
  if (!name) throw new Error('Enter your name first.');
  localStorage.setItem('listen-together-user', name);
  setProfile(name);
  return name;
}

function showError(message) {
  formError.textContent = message;
}

function setConnection(connected, label) {
  connectionPill.classList.toggle('connected', connected);
  connectionLabel.textContent = label;
}

function connect() {
  return new Promise((resolve, reject) => {
    if (socket?.readyState === WebSocket.OPEN) return resolve();
    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${scheme}//${location.host}`);
    setConnection(false, 'CONNECTING');
    clockSamples = [];
    clockOffsetMs = 0;
    socket.addEventListener('open', () => {
      setConnection(true, 'CONNECTED');
      showError('');
      syncClock();
      clearInterval(clockSyncTimer);
      clockSyncTimer = setInterval(syncClock, 30_000);
      resolve();
    }, { once: true });
    socket.addEventListener('error', () => {
      setConnection(false, 'CONNECTION ERROR');
      reject(new Error('Could not connect to the room server. Check that npm start is running.'));
    }, { once: true });
    socket.addEventListener('close', () => {
      clearInterval(clockSyncTimer);
      setConnection(false, 'DISCONNECTED');
      for (const { reject: fail, timeout } of pendingAcks.values()) {
        clearTimeout(timeout);
        fail(new Error('Connection lost before the command was confirmed.'));
      }
      pendingAcks.clear();
    });
    socket.addEventListener('message', (event) => {
      try {
        handleMessage(JSON.parse(event.data));
      } catch {
        showNotice('Could not read a message from the room server.', true);
      }
    });
  });
}

function send(message) {
  if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error('Not connected to the room.');
  socket.send(JSON.stringify(message));
}

function syncClock() {
  if (socket?.readyState === WebSocket.OPEN) {
    try { send({ t: 'TIME', t0: Date.now() }); } catch (error) { showNotice(error.message, true); }
  }
}

function estimatedServerNow() {
  return Date.now() + clockOffsetMs;
}

function handleMessage(message) {
  switch (message.t) {
    case 'TIME_REPLY': {
      const t3 = Date.now();
      const rtt = t3 - message.t0 - (message.t2 - message.t1);
      if (!Number.isFinite(rtt) || rtt < 0) break;
      const offset = ((message.t1 - message.t0) + (message.t2 - t3)) / 2;
      clockSamples.push({ rtt, offset });
      if (clockSamples.length > 8) clockSamples.shift();
      clockOffsetMs = clockSamples.reduce((best, sample) => sample.rtt < best.rtt ? sample : best).offset;
      break;
    }
    case 'SNAPSHOT':
      roomId = message.roomId;
      inviteToken = message.inviteToken || inviteToken;
      roomState = message.state;
      knownPeople.set(roomState.hostId, roomState.hostId);
      showRoom();
      break;
    case 'EVENTS':
      for (const event of message.events) applyEvent(event);
      break;
    case 'EVENT':
      applyEvent(message.e);
      break;
    case 'ACK': {
      const pending = pendingAcks.get(message.commandId);
      if (pending) {
        clearTimeout(pending.timeout);
        pendingAcks.delete(message.commandId);
        if (message.ok) pending.resolve(message);
        else pending.reject(new Error(commandError(message.error)));
      }
      break;
    }
    case 'ERROR':
      showNotice(message.error === 'BAD_INVITE' ? 'That invite link is invalid.' : 'This room is no longer available.', true);
      showError(message.error === 'BAD_INVITE' ? 'That invite link is invalid.' : 'This room is no longer available.');
      break;
  }
}

function applyEvent(event) {
  if (!roomState || event.epoch < roomState.epoch || event.version <= roomState.version) return;
  if (event.version !== roomState.version + 1) {
    send({ t: 'JOIN', roomId, inviteToken, userId: userId() });
    showNotice('Resyncing room state…');
    return;
  }
  switch (event.type) {
    case 'ROOM_CREATED':
      roomState.hostId = event.hostId;
      break;
    case 'QUEUE_ADDED':
      roomState.queue.push(event.item);
      roomState.queueVersion += 1;
      roomState.nextSeq = event.item.seq + 1;
      knownPeople.set(event.item.addedBy, event.item.addedBy);
      break;
    case 'VOTE_CAST':
      if (!roomState.votes.includes(event.userId)) roomState.votes.push(event.userId);
      knownPeople.set(event.userId, event.userId);
      break;
    case 'PLAYBACK':
      roomState.playback = event.playback;
      if (event.popQueue) {
        roomState.queue.shift();
        roomState.queueVersion += 1;
      }
      if (event.resetVotes) roomState.votes = [];
      break;
  }
  roomState.version = event.version;
  roomState.epoch = Math.max(roomState.epoch, event.epoch);
  renderRoom();
}

function commandError(code) {
  const errors = {
    FORBIDDEN: 'Only the host can control playback.',
    STALE_STATE: 'The room changed. Please try again.',
    EMPTY_QUEUE: 'Add a track before starting playback.',
    NOT_PLAYING: 'There is no active track to seek.',
    QUEUE_FULL: 'The shared queue is full.',
    QUOTA: 'You have reached the demo queue limit.',
    BAD_INVITE: 'That invite link is invalid.',
    NO_ROOM: 'This room is no longer available.'
  };
  return errors[code] || `The command could not be completed${code ? ` (${code})` : ''}.`;
}

function makeId() {
  return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function sendCommand(type, fields = {}) {
  const commandId = makeId();
  send({ t: 'CMD', cmd: { type, commandId, ...fields } });
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      pendingAcks.delete(commandId);
      reject(new Error('The server did not confirm that action. Try again.'));
    }, 5000);
    pendingAcks.set(commandId, { resolve, reject, timeout });
  });
}

async function createRoom() {
  try {
    const name = userId();
    await connect();
    send({ t: 'CREATE', userId: name });
  } catch (error) {
    showError(error.message);
  }
}

function parseInvite(value) {
  const trimmed = value.trim();
  if (!trimmed) throw new Error('Paste a room invite link or enter a room ID.');
  try {
    const url = new URL(trimmed, location.origin);
    const id = url.searchParams.get('room') || url.pathname.match(/(?:room|join)\/([^/]+)/)?.[1];
    const token = url.searchParams.get('invite') || url.searchParams.get('token') || '';
    if (id) return { roomId: id, inviteToken: token };
  } catch {}
  const [id, token] = trimmed.split(/[?#]/, 2);
  if (!id || !token) throw new Error('Use a complete invite link from the host.');
  return { roomId: id, inviteToken: token };
}

async function joinRoom() {
  try {
    const name = userId();
    const invite = params.get('room') && params.get('invite')
      ? { roomId: params.get('room'), inviteToken: params.get('invite') }
      : parseInvite(document.querySelector('#invite-link').value);
    await connect();
    send({ t: 'JOIN', ...invite, userId: name });
  } catch (error) {
    showError(error.message);
  }
}

function showRoom() {
  welcome.classList.add('hidden');
  roomView.classList.remove('hidden');
  history.replaceState(null, '', `/?room=${encodeURIComponent(roomId)}&invite=${encodeURIComponent(inviteToken)}`);
  renderRoom();
}

function showNotice(message, isError = false) {
  notice.textContent = message;
  notice.classList.toggle('error', isError);
  if (message && !isError) setTimeout(() => {
    if (notice.textContent === message) notice.textContent = '';
  }, 3500);
}

function formatTime(ms) {
  const seconds = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

function currentPosition() {
  const playback = roomState.playback;
  if (playback.status !== 'playing') return playback.positionMs;
  return playback.positionMs + Math.max(0, estimatedServerNow() - playback.serverTimestamp);
}

function youtubeVideoId(value) {
  try {
    const url = new URL(value);
    const host = url.hostname.toLowerCase().replace(/^www\./, '');
    let id = '';
    if (host === 'youtu.be') id = url.pathname.split('/').filter(Boolean)[0] || '';
    else if (['youtube.com', 'm.youtube.com', 'music.youtube.com', 'youtube-nocookie.com'].includes(host)) {
      if (url.pathname === '/watch') id = url.searchParams.get('v') || '';
      else id = url.pathname.match(/^\/(?:embed|shorts|live)\/([^/?]+)/)?.[1] || '';
    }
    return /^[A-Za-z0-9_-]{11}$/.test(id) ? id : '';
  } catch {
    return '';
  }
}

function mediaSource(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  if (!['http:', 'https:'].includes(url.protocol)) return null;

  const youtubeId = youtubeVideoId(value);
  if (youtubeId) return { type: 'youtube', id: youtubeId, url };

  const host = url.hostname.toLowerCase().replace(/^www\./, '');
  if (host === 'open.spotify.com' || host === 'play.spotify.com') {
    const match = url.pathname.match(/^\/(?:intl-[a-z]{2}\/)?(?:embed\/)?(track|episode)\/([A-Za-z0-9]+)\/?$/);
    if (!match) return { type: 'unsupported', message: 'Use a Spotify track or episode link. Spotify albums, playlists and other pages are not playable in this room.' };
    return { type: 'spotify', id: match[2], kind: match[1], url };
  }

  const extension = url.pathname.match(/\.([a-z0-9]+)$/i)?.[1]?.toLowerCase();
  const audioExtensions = new Set(['mp3', 'm4a', 'aac', 'ogg', 'oga', 'opus', 'wav', 'flac']);
  const videoExtensions = new Set(['mp4', 'm4v', 'webm', 'ogv']);
  if (audioExtensions.has(extension)) return { type: 'direct-audio', url };
  if (videoExtensions.has(extension)) return { type: 'direct-video', url };
  return { type: 'unsupported', message: 'This link is not a supported audio source. Use YouTube, a Spotify track/episode, or a direct MP3/MP4 media URL.' };
}

function setYoutubeStatus(message, isError = false) {
  const status = document.querySelector('#youtube-status');
  status.textContent = message;
  status.classList.toggle('error', isError);
}

function currentMediaDurationMs() {
  const currentTrack = roomState?.queue[0]?.trackId;
  const source = currentTrack ? mediaSource(currentTrack) : null;
  if (source?.type === 'youtube' && youtubePlayer && youtubePlayer.getVideoData?.().video_id === source.id) {
    const duration = youtubePlayer.getDuration?.() || 0;
    if (duration > 0) return duration * 1000;
  }
  if (source?.type === 'direct-audio' || source?.type === 'direct-video') {
    const directPlayer = document.querySelector(source.type === 'direct-video' ? '#direct-video' : '#direct-audio');
    if (directPlayer.readyState >= 1 && Number.isFinite(directPlayer.duration)) return directPlayer.duration * 1000;
  }
  return roomState?.queue[0]?.durationMs || 0;
}

function onYoutubeReady() {
  youtubePlayerIsReady = true;
  syncMediaPlayer();
}

function onYoutubeStateChange(event) {
  if (event.data === window.YT.PlayerState.PLAYING) {
    const title = youtubePlayer.getVideoData()?.title;
    if (title) {
      youtubeTitle = title;
      document.querySelector('#track-title').textContent = title;
    }
    setYoutubeStatus('Playing from YouTube. Room controls keep everyone aligned.');
  } else if (event.data === window.YT.PlayerState.BUFFERING) {
    setYoutubeStatus('Buffering from YouTube…');
  } else if (event.data === window.YT.PlayerState.CUED && roomState?.playback.status === 'playing') {
    syncMediaPlayer();
  }
  if (roomState?.playback.status === 'playing' && event.data === window.YT.PlayerState.PAUSED) {
    syncMediaPlayer();
  }
}

function onYoutubeError(event) {
  const messages = {
    2: 'This YouTube link is invalid. Add a regular YouTube video link.',
    5: 'YouTube could not play this video in the browser player.',
    100: 'This YouTube video is unavailable or has been removed.',
    101: 'The video owner does not allow playback in embedded players. Try another video.',
    150: 'The video owner does not allow playback in embedded players. Try another video.',
    153: 'YouTube could not verify the playback request. Open this app from its http://localhost address.'
  };
  setYoutubeStatus(messages[event.data] || `YouTube could not play this video (error ${event.data}).`, true);
}

function showOnlyMediaPanel(id) {
  for (const panelId of ['youtube-panel', 'spotify-panel', 'direct-panel']) {
    document.querySelector(`#${panelId}`).classList.toggle('hidden', panelId !== id);
  }
}

function stopSpotifyEmbed() {
  if (!activeSpotifyUri) return;
  document.querySelector('#spotify-player').src = 'about:blank';
  activeSpotifyUri = '';
}

function syncDirectPlayer(source, playback) {
  const isVideo = source.type === 'direct-video';
  const player = document.querySelector(isVideo ? '#direct-video' : '#direct-audio');
  const otherPlayer = document.querySelector(isVideo ? '#direct-audio' : '#direct-video');
  const changed = activeDirectUrl !== source.url.href || directPlayerKind !== source.type;
  if (changed) {
    otherPlayer.pause();
    otherPlayer.removeAttribute('src');
    otherPlayer.load();
  }
  player.classList.toggle('hidden', false);
  otherPlayer.classList.toggle('hidden', true);
  document.querySelector('#open-direct').href = source.url.href;

  if (changed) {
    activeDirectUrl = source.url.href;
    directPlayerKind = source.type;
    directPlayBlocked = false;
    player.src = source.url.href;
    player.load();
    document.querySelector('#enable-audio').classList.toggle('hidden', audioEnabled);
    document.querySelector('#enable-audio').disabled = false;
    document.querySelector('#enable-audio').textContent = audioEnabled ? 'Audio enabled' : 'Enable synchronized media';
    document.querySelector('#direct-status').textContent = isVideo
      ? audioEnabled ? 'Room controls schedule the MP4 video for everyone.' : 'Click Enable once. Room controls schedule the MP4 video for everyone.'
      : audioEnabled ? 'Room controls schedule audio-only MP3 playback for everyone.' : 'Click Enable once. Room controls schedule audio-only MP3 playback for everyone.';
    document.querySelector('#direct-status').classList.remove('error');
    return;
  }

  const targetSeconds = currentPosition() / 1000;
  const actualSeconds = player.currentTime || 0;
  const driftSeconds = actualSeconds - targetSeconds;
  if (player.readyState >= 1 && Math.abs(driftSeconds) > 0.3) {
    try { player.currentTime = targetSeconds; } catch {}
    player.playbackRate = 1;
  } else if (playback.status === 'playing' && player.readyState >= 2 && Math.abs(driftSeconds) > 0.04) {
    player.playbackRate = driftSeconds > 0 ? 0.97 : 1.03;
  } else {
    player.playbackRate = 1;
  }
  if (playback.status === 'playing') {
    if (estimatedServerNow() < playback.serverTimestamp) {
      if (!player.paused) player.pause();
      return;
    }
    if (player.paused && audioEnabled && !directPlayBlocked) {
      player.play().catch(() => {
        directPlayBlocked = true;
        audioEnabled = false;
        const status = document.querySelector('#direct-status');
        status.textContent = 'Browser blocked playback. Click Enable synchronized media to allow it.';
        status.classList.add('error');
        document.querySelector('#enable-audio').classList.remove('hidden');
        document.querySelector('#enable-audio').disabled = false;
      });
    }
  } else if (!player.paused) {
    player.pause();
  }
}

function syncMediaPlayer() {
  if (!roomState) return;
  const track = roomState.queue[0];
  const source = track ? mediaSource(track.trackId) : null;
  if (!source || source.type === 'unsupported') {
    if (activeYoutubeId && youtubePlayer?.getPlayerState?.() === window.YT?.PlayerState.PLAYING) youtubePlayer.pauseVideo();
    stopSpotifyEmbed();
    document.querySelector('#direct-audio').pause();
    document.querySelector('#direct-video').pause();
    activeYoutubeId = '';
    activeSpotifyUri = '';
    activeDirectUrl = '';
    directPlayerKind = '';
    youtubeTitle = '';
    showOnlyMediaPanel('');
    if (source?.type === 'unsupported') setYoutubeStatus(source.message, true);
    else setYoutubeStatus('Add a supported YouTube, Spotify track/episode, or direct MP3/MP4 link to play audio.');
    return;
  }

  if (source.type !== 'youtube' && activeYoutubeId && youtubePlayer?.getPlayerState?.() === window.YT?.PlayerState.PLAYING) youtubePlayer.pauseVideo();
  if (source.type !== 'direct-audio' && activeDirectUrl) document.querySelector('#direct-audio').pause();
  if (source.type !== 'direct-video' && activeDirectUrl) document.querySelector('#direct-video').pause();

  if (source.type === 'youtube') {
    stopSpotifyEmbed();
    activeSpotifyUri = '';
    activeDirectUrl = '';
    directPlayerKind = '';
    document.querySelector('#direct-audio').pause();
    document.querySelector('#direct-video').pause();
    showOnlyMediaPanel('youtube-panel');
    if (source.id !== activeYoutubeId) {
      activeYoutubeId = source.id;
      requestedYoutubeId = '';
      youtubeTitle = '';
      playAttemptKey = '';
      setYoutubeStatus('Loading the official YouTube player…');
    }
    document.querySelector('#open-youtube').href = `https://www.youtube.com/watch?v=${source.id}`;
    if (!youtubeApiReady || !window.YT?.Player) return;

    if (!youtubePlayer) {
      requestedYoutubeId = source.id;
      youtubePlayer = new window.YT.Player('youtube-player', {
        width: '100%',
        height: '100%',
        videoId: source.id,
        playerVars: {
          autoplay: 0,
          controls: 1,
          enablejsapi: 1,
          origin: location.origin,
          playsinline: 1,
          rel: 0
        },
        events: {
          onReady: onYoutubeReady,
          onStateChange: onYoutubeStateChange,
          onError: onYoutubeError,
          onAutoplayBlocked: () => setYoutubeStatus('Your browser blocked autoplay. Press Play in the YouTube player below to hear the room.', true)
        }
      });
      return;
    }

    if (!youtubePlayerReady()) return;
    if (youtubePlayer.getVideoData()?.video_id !== source.id && requestedYoutubeId !== source.id) {
      requestedYoutubeId = source.id;
      youtubePlayer.cueVideoById(source.id);
      return;
    }
    if (youtubePlayer.getVideoData()?.video_id !== source.id) return;

    const playback = roomState.playback;
    const state = window.YT.PlayerState;
    const actualState = youtubePlayer.getPlayerState();
    const targetSeconds = currentPosition() / 1000;
    const playerSeconds = youtubePlayer.getCurrentTime() || 0;
    if (Math.abs(playerSeconds - targetSeconds) > 1.25 && playback.trackId) youtubePlayer.seekTo(targetSeconds, true);

    if (playback.status === 'playing') {
      const key = `${playback.trackInstanceId}:${playback.serverTimestamp}`;
      if (actualState !== state.PLAYING && playAttemptKey !== key) {
        playAttemptKey = key;
        youtubePlayer.playVideo();
      }
    } else {
      playAttemptKey = '';
      if (actualState === state.PLAYING || actualState === state.BUFFERING) youtubePlayer.pauseVideo();
      if (playback.status === 'paused' && Math.abs(playerSeconds - targetSeconds) > 1.25) youtubePlayer.seekTo(targetSeconds, true);
    }
    return;
  }

  if (source.type === 'spotify') {
    activeDirectUrl = '';
    directPlayerKind = '';
    document.querySelector('#direct-audio').pause();
    document.querySelector('#direct-video').pause();
    showOnlyMediaPanel('spotify-panel');
    const uri = `spotify:embed:${source.kind}:${source.id}`;
    const frame = document.querySelector('#spotify-player');
    document.querySelector('#open-spotify').href = `https://open.spotify.com/${source.kind}/${source.id}`;
    if (uri !== activeSpotifyUri) {
      activeSpotifyUri = uri;
      frame.src = `https://open.spotify.com/embed/${source.kind}/${source.id}?utm_source=generator&theme=0`;
      document.querySelector('#spotify-status').textContent = 'Press Play in the Spotify player. Account and region restrictions may apply. Spotify embed playback is separate and cannot be synchronized by this demo room.';
    }
    return;
  }

  stopSpotifyEmbed();
  showOnlyMediaPanel('direct-panel');
  syncDirectPlayer(source, roomState.playback);
}

function youtubePlayerReady() {
  return youtubePlayerIsReady && !!youtubePlayer?.getPlayerState;
}

function renderRoom() {
  if (!roomState) return;
  const state = roomState;
  const playback = state.playback;
  const isHost = state.hostId === userInput.value.trim();
  const track = state.queue[0];
  const listeners = Math.max(1, knownPeople.size);
  document.querySelector('#room-title').textContent = `${state.hostId}'s room`;
  document.querySelector('#room-version').textContent = `Version ${state.version}`;
  document.querySelector('#member-count').textContent = `${listeners} ${listeners === 1 ? 'listener' : 'listeners'}`;
  document.querySelector('#aside-member-count').textContent = String(listeners).padStart(2, '0');
  const source = playback.trackId ? mediaSource(playback.trackId) : null;
  const title = source?.type === 'youtube'
    ? 'YouTube video'
    : source?.type === 'spotify'
      ? 'Spotify track'
      : source?.type === 'direct-audio' || source?.type === 'direct-video'
        ? directMediaLabel(source.url)
        : playback.trackId || 'Nothing playing yet';
  document.querySelector('#track-title').textContent = (youtubeTitle && source?.type === 'youtube') || title;
  document.querySelector('#track-artist').textContent = playback.status === 'idle'
    ? 'Add a supported media link to start listening'
    : `${source?.type === 'youtube' ? 'YouTube' : source?.type === 'spotify' ? 'Spotify embed' : source?.type?.startsWith('direct-') ? 'Direct media' : 'Shared room'} · ${playback.status === 'paused' ? 'Paused for everyone' : 'Shared room playback'}`;
  document.querySelector('#queue-total').textContent = String(state.queue.length);
  document.querySelector('#vote-count').textContent = `${state.votes.length} ${state.votes.length === 1 ? 'vote' : 'votes'}`;
  const seek = document.querySelector('#seek-bar');
  seek.disabled = !isHost || !track;
  renderPlaybackProgress();
  document.querySelector('#play-icon').textContent = playback.status === 'playing' ? 'Ⅱ' : '▶';
  document.querySelector('#playback-control').disabled = !isHost;
  document.querySelector('#playback-control').style.opacity = isHost ? '1' : '.45';
  document.querySelector('#playback-control').title = playback.status === 'playing' ? 'Pause for everyone' : 'Play for everyone';
  document.querySelector('#host-control-label').textContent = isHost ? 'ROOM CONTROLS' : 'LISTENER';
  document.querySelector('#enable-audio').classList.toggle('hidden', audioEnabled);
  const voteButton = document.querySelector('#vote-skip');
  const alreadyVoted = state.votes.includes(userInput.value.trim());
  voteButton.disabled = playback.status === 'idle' || alreadyVoted;
  voteButton.textContent = alreadyVoted ? 'Vote cast' : 'Vote to skip';
  document.querySelector('#next-track').classList.toggle('hidden', !isHost);
  document.querySelector('#next-track').disabled = playback.status === 'idle';
  document.querySelector('#people-list').innerHTML = [...knownPeople.entries()].map(([id, name]) =>
    `<div class="person-row"><span class="person-avatar">${escapeHtml(name.charAt(0).toUpperCase())}</span><span class="person-name">${escapeHtml(name)}${id === userInput.value.trim() ? ' (you)' : ''}</span>${id === state.hostId ? '<span class="host-badge">HOST</span>' : ''}</div>`
  ).join('');
  document.querySelector('#queue-list').innerHTML = state.queue.length
    ? state.queue.map((item, index) => {
      const itemSource = mediaSource(item.trackId);
      const itemTitle = itemSource?.type === 'youtube'
        ? 'YouTube video'
        : itemSource?.type === 'spotify'
          ? 'Spotify track'
          : itemSource?.type === 'direct-audio' || itemSource?.type === 'direct-video'
            ? directMediaLabel(itemSource.url)
            : item.trackId;
      return `<li class="queue-item ${index === 0 && playback.status !== 'idle' ? 'current' : ''}"><span class="queue-index">${index === 0 && playback.status !== 'idle' ? '♫' : String(index + 1).padStart(2, '0')}</span><span class="queue-track"><strong>${escapeHtml(itemTitle)}</strong><span>${index === 0 && playback.status !== 'idle' ? 'Now playing' : `Added by ${escapeHtml(item.addedBy)}`}</span></span><span class="queue-duration">${formatTime(item.durationMs)}</span></li>`;
    }).join('')
    : '<li class="empty-queue">The queue is open. Add the first track.</li>';
  syncMediaPlayer();
}

function renderPlaybackProgress() {
  if (!roomState) return;
  const totalMs = currentMediaDurationMs();
  const position = Math.min(currentPosition(), totalMs || currentPosition());
  document.querySelector('#seek-bar').value = totalMs ? String(Math.min(100, Math.round(position / totalMs * 100))) : '0';
  document.querySelector('#elapsed-time').textContent = formatTime(position);
  document.querySelector('#total-time').textContent = formatTime(totalMs);
}

function directMediaLabel(url) {
  const name = url.pathname.split('/').filter(Boolean).pop() || 'Direct media';
  try { return decodeURIComponent(name); } catch { return name; }
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
}

document.querySelector('#create-room').addEventListener('click', createRoom);
document.querySelector('#join-room').addEventListener('click', joinRoom);
userInput.addEventListener('keydown', (event) => { if (event.key === 'Enter') createRoom(); });
document.querySelector('#invite-link').addEventListener('keydown', (event) => { if (event.key === 'Enter') joinRoom(); });

document.querySelector('#add-track-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const trackId = document.querySelector('#track-id').value.trim();
  const durationMs = Number(document.querySelector('#track-duration').value) * 1000;
  if (!trackId || !Number.isFinite(durationMs) || durationMs < 1000 || durationMs > 3_600_000) return showNotice('Enter a track and a duration between 1 and 3600 seconds.', true);
  const source = mediaSource(trackId);
  if (!source || source.type === 'unsupported') return showNotice(source?.message || 'Paste a supported YouTube, Spotify track/episode, or direct audio/video URL.', true);
  try {
    await sendCommand('QUEUE_ADD', { trackId, durationMs });
    document.querySelector('#track-id').value = '';
    showNotice('Added to the shared queue.');
  } catch (error) { showNotice(error.message, true); }
});

document.querySelector('#playback-control').addEventListener('click', async () => {
  const type = roomState?.playback.status === 'playing' ? 'PAUSE' : roomState?.playback.status === 'paused' ? 'RESUME' : 'PLAY';
  try {
    await sendCommand(type);
    showNotice(type === 'PAUSE' ? 'Paused for everyone.' : 'Playback started for the room.');
  } catch (error) { showNotice(error.message, true); }
});

document.querySelector('#enable-audio').addEventListener('click', async () => {
  const source = roomState?.queue[0] ? mediaSource(roomState.queue[0].trackId) : null;
  if (source?.type !== 'direct-audio' && source?.type !== 'direct-video') {
    showNotice('Enable audio is available for direct MP3/MP4 media.', true);
    return;
  }
  audioEnabled = true;
  directPlayBlocked = false;
  const player = document.querySelector(source.type === 'direct-video' ? '#direct-video' : '#direct-audio');
  const playback = roomState.playback;
  if (playback.status === 'playing' && estimatedServerNow() >= playback.serverTimestamp) {
    try {
      await player.play();
      const drift = Math.abs(player.currentTime - currentPosition() / 1000);
      if (drift > 0.3) player.currentTime = currentPosition() / 1000;
      document.querySelector('#direct-status').textContent = 'Synchronized playback enabled for this listener.';
      document.querySelector('#direct-status').classList.remove('error');
      document.querySelector('#enable-audio').classList.add('hidden');
    } catch {
      audioEnabled = false;
      const status = document.querySelector('#direct-status');
      status.textContent = 'Playback is blocked. Check the media URL, then click Enable again.';
      status.classList.add('error');
    }
  } else {
    document.querySelector('#direct-status').textContent = 'Audio enabled. It will start with the room when the host presses Play.';
    document.querySelector('#enable-audio').textContent = 'Media enabled';
    document.querySelector('#enable-audio').disabled = true;
  }
});

document.querySelector('#vote-skip').addEventListener('click', async () => {
  try {
    const result = await sendCommand('SKIP_VOTE', { trackInstanceId: roomState.playback.trackInstanceId });
    if (result.skipTriggered) {
      showNotice(result.nextTrackAvailable
        ? 'Vote passed. Skipped to the next track for everyone.'
        : 'Vote passed, but there is no next track. Add one to the queue.');
    } else if (result.alreadyVoted) {
      showNotice(`Your vote is already counted (${result.voteCount}/${result.voteThreshold}).`);
    } else {
      showNotice(`Vote recorded (${result.voteCount}/${result.voteThreshold} needed). Waiting for more votes.`);
    }
  } catch (error) { showNotice(error.message, true); }
});

document.querySelector('#next-track').addEventListener('click', async () => {
  try {
    await sendCommand('SKIP', { trackInstanceId: roomState.playback.trackInstanceId });
    showNotice('Skipped to the next track for everyone.');
  } catch (error) { showNotice(error.message, true); }
});

document.querySelector('#seek-bar').addEventListener('input', () => {
  seeking = true;
  const duration = currentMediaDurationMs();
  document.querySelector('#elapsed-time').textContent = formatTime(duration * Number(document.querySelector('#seek-bar').value) / 100);
});
document.querySelector('#seek-bar').addEventListener('change', async () => {
  const duration = currentMediaDurationMs();
  const positionMs = Math.round(duration * Number(document.querySelector('#seek-bar').value) / 100);
  seeking = false;
  try {
    await sendCommand('SEEK', { positionMs, trackInstanceId: roomState.playback.trackInstanceId });
    showNotice('Seeked for everyone.');
  } catch (error) { showNotice(error.message, true); renderRoom(); }
});

document.querySelector('#copy-invite').addEventListener('click', async () => {
  const link = `${location.origin}/?room=${encodeURIComponent(roomId)}&invite=${encodeURIComponent(inviteToken)}`;
  try {
    await navigator.clipboard.writeText(link);
    showNotice('Invite link copied. Send it to your friends.');
  } catch {
    window.prompt('Copy this room invite link:', link);
  }
});

document.querySelector('#leave-room').addEventListener('click', () => {
  socket?.close();
  if (youtubePlayer && youtubePlayerReady()) youtubePlayer.stopVideo();
  document.querySelector('#direct-audio').pause();
  document.querySelector('#direct-video').pause();
  audioEnabled = false;
  stopSpotifyEmbed();
  roomState = undefined;
  roomId = '';
  inviteToken = '';
  knownPeople = new Map();
  roomView.classList.add('hidden');
  welcome.classList.remove('hidden');
  history.replaceState(null, '', '/');
  setConnection(false, 'NOT CONNECTED');
});

setInterval(() => {
  if (roomState && !seeking) {
    renderPlaybackProgress();
    syncMediaPlayer();
  }
}, 100);
