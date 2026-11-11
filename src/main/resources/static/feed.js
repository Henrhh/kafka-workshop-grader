// Projector page: live feed, leaderboard and level board.
const MAX_CARDS = 80;
const feedEl = document.getElementById('feed');
const cards = new Map();

document.getElementById('join-link').textContent = location.host + '/join.html';

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function time(at) {
  return new Date(at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function prepend(card) {
  feedEl.prepend(card);
  while (feedEl.children.length > MAX_CARDS) {
    const last = feedEl.lastElementChild;
    if (last.dataset.attempt) cards.delete(last.dataset.attempt);
    last.remove();
  }
}

function simpleCard(className, who, message, at) {
  const card = el('div', 'card ' + className);
  const head = el('div', 'head');
  head.append(el('span', 'who', who), el('span', 'when', time(at)));
  card.append(head, el('div', 'msg', message));
  prepend(card);
}

function attemptCard(event) {
  let card = cards.get(event.attemptId);
  if (!card) {
    card = el('div', 'card');
    card.dataset.attempt = event.attemptId;
    const head = el('div', 'head');
    head.append(el('span', 'who', `${event.team} · level ${event.level}`), el('span', 'when', time(event.at)));
    card.append(head, el('ul'), el('div', 'running', 'grading…'));
    cards.set(event.attemptId, card);
    prepend(card);
  }
  return card;
}

function handle(event, live) {
  switch (event.type) {
    case 'TEAM_JOINED':
      simpleCard('joined', event.team, event.message, event.at);
      break;
    case 'ANNOUNCEMENT':
      simpleCard('announcement', 'Announcement', event.message, event.at);
      break;
    case 'ATTEMPT_STARTED':
      attemptCard(event);
      break;
    case 'CHECK_PASSED':
    case 'CHECK_FAILED': {
      const card = attemptCard(event);
      card.querySelector('ul').append(el('li', event.type === 'CHECK_PASSED' ? 'pass' : 'fail', event.message));
      break;
    }
    case 'SOLVED':
    case 'FAILED': {
      const card = attemptCard(event);
      card.classList.add(event.type === 'SOLVED' ? 'solved' : 'failed');
      card.querySelector('.running')?.remove();
      const text = event.type === 'SOLVED' ? event.message : 'Not yet: ' + event.message;
      card.append(el('div', 'result', text));
      // Failed attempts collapse to the failing check, so the feed stays readable.
      if (event.type === 'FAILED' && card.querySelector('li.fail')) {
        card.querySelectorAll('li.pass').forEach(li => li.remove());
        card.querySelector('.result').textContent = 'Not yet';
      }
      if (live) refresh();
      break;
    }
  }
}

async function getJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(path + ': ' + response.status);
  return response.json();
}

async function refreshLeaderboard() {
  const rows = await getJson('/api/leaderboard');
  const body = document.getElementById('leaderboard');
  body.replaceChildren(...rows.map((row, i) => {
    const tr = el('tr', i === 0 && row.points > 0 ? 'first' : '');
    tr.append(el('td', '', String(i + 1)), el('td', '', row.team),
      el('td', 'num', String(row.levelsSolved)), el('td', 'num', String(row.points)));
    return tr;
  }));
}

async function refreshBoard() {
  const board = await getJson('/api/board');
  const head = el('tr');
  head.append(el('th', '', 'Team'), ...board.levels.map(l => el('th', 'cell', String(l.level))));
  document.getElementById('board-head').replaceChildren(head);

  const solved = new Map(board.solves.map(s => [s.team + '/' + s.level, s]));
  const failed = new Map(board.failed.map(f => [f.team + '/' + f.level, f.failed]));
  const active = new Map(Object.entries(board.active).map(([team, value]) => [team, Number(value.split(':')[1])]));
  const medals = { 1: '🥇', 2: '🥈', 3: '🥉' };

  document.getElementById('board').replaceChildren(...board.teams.map(team => {
    const tr = el('tr');
    tr.append(el('td', '', team));
    for (const { level } of board.levels) {
      const key = team + '/' + level;
      const td = el('td', 'cell');
      const span = el('span');
      if (solved.has(key)) {
        td.classList.add('solved');
        span.textContent = medals[solved.get(key).rank] || '✓';
      } else if (failed.has(key)) {
        td.classList.add('failed');
        span.textContent = String(failed.get(key));
      }
      if (active.get(team) === level) td.classList.add('active');
      td.append(span);
      tr.append(td);
    }
    return tr;
  }));
}

let refreshTimer;
function refresh() {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => {
    refreshLeaderboard().catch(console.error);
    refreshBoard().catch(console.error);
  }, 300);
}

async function start() {
  const history = await getJson('/api/feed');
  history.forEach(event => handle(event, false));
  refresh();

  const status = document.getElementById('status');
  const source = new EventSource('/api/feed/stream');
  source.addEventListener('open', () => { status.textContent = 'live'; });
  source.addEventListener('error', () => { status.textContent = 'reconnecting…'; });
  source.addEventListener('event', message => {
    const event = JSON.parse(message.data);
    handle(event, true);
    if (event.type === 'ATTEMPT_STARTED' || event.type === 'TEAM_JOINED') refresh();
  });
  setInterval(refresh, 15000);
}

start().catch(error => {
  document.getElementById('status').textContent = 'error: ' + error.message;
});
