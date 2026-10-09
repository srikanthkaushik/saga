'use strict';

/*
 * Saga console. Talks to the services through the saga-ui proxy (/api/{order|payment|inventory}/...) and to
 * saga-ui itself for message injection (/api/inject). All server data is rendered via textContent / DOM nodes.
 */

const SERVICES = ['order', 'payment', 'inventory'];
const LANE = { order: 1, payment: 2, inventory: 3 };
const TOPIC_TARGET = { 'payment.commands': 'payment', 'inventory.commands': 'inventory', 'order.saga.replies': 'order' };
const POLL_MS = 1500;

const state = {
    tab: 'saga',
    orderId: null,
    sagaId: null,
    mapKey: null,
    busy: new Set(),
    ticks: 0,
};

/* ------------------------------------------------------------------ helpers */

const $ = (selector) => document.querySelector(selector);

function h(tag, props = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(props || {})) {
        if (value === undefined || value === null || value === false) continue;
        if (key === 'class') node.className = value;
        else if (key === 'text') node.textContent = value;
        else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
        else node.setAttribute(key, value === true ? '' : value);
    }
    for (const child of children.flat()) {
        if (child === null || child === undefined || child === false) continue;
        node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
}

function svg(tag, attrs = {}, text) {
    const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
    if (text !== undefined) node.textContent = text;
    return node;
}

async function call(service, method, path, body) {
    const url = service === 'ui' ? path : `/api/${service}${path}`;
    const init = { method, headers: { Accept: 'application/json, text/plain, */*' } };
    if (body !== undefined && body !== null && body !== '') {
        init.headers['Content-Type'] = 'application/json';
        init.body = typeof body === 'string' ? body : JSON.stringify(body);
    }
    const started = performance.now();
    let response;
    try {
        response = await fetch(url, init);
    } catch (e) {
        return { ok: false, status: 0, data: { error: 'The saga console backend is not reachable. Is saga-ui running?' }, text: String(e), ms: 0 };
    }
    const text = await response.text();
    let data = text;
    try { data = text ? JSON.parse(text) : null; } catch { /* not JSON: keep text */ }
    return { ok: response.ok, status: response.status, data, text, ms: Math.round(performance.now() - started) };
}

const get = (service, path) => call(service, 'GET', path);

function errorText(result) {
    const d = result.data;
    if (d && typeof d === 'object') {
        if (d.error && d.error !== 'Bad Request' && d.error !== 'Not Found') return d.error;
        if (d.detail) return d.detail;
        if (d.message) return d.message;
    }
    if (result.status === 400) return 'The request was rejected as invalid (400). Check the values.';
    if (result.status === 404) return 'Not found (404).';
    return `Request failed with HTTP ${result.status}.`;
}

async function once(name, fn) {
    if (state.busy.has(name)) return;
    state.busy.add(name);
    try { await fn(); } catch (e) { console.error(name, e); } finally { state.busy.delete(name); }
}

const short = (id) => (id ? String(id).slice(0, 8) : '');
const money = (v) => (v === null || v === undefined ? '–' : Number(v).toFixed(2));
const human = (s) => String(s || '').toLowerCase().replaceAll('_', ' ');
const uuid = () => crypto.randomUUID();

function ago(iso) {
    if (!iso) return '';
    const s = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 1000));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    return `${Math.floor(s / 3600)}h ago`;
}

function offset(iso, baseIso) {
    const ms = Date.parse(iso) - Date.parse(baseIso);
    if (Number.isNaN(ms)) return '';
    return ms < 10000 ? `+${Math.max(0, ms)} ms` : `+${(ms / 1000).toFixed(1)} s`;
}

function duration(seconds) {
    const s = Math.round(seconds);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

const badge = (value) => h('span', { class: `badge ${value || ''}`, text: human(value) || '–' });

function facts(dl, pairs) {
    dl.replaceChildren(...pairs.flatMap(([term, value]) => [h('dt', { text: term }), h('dd', {}, value ?? '–')]));
}

function store(key, value) { try { localStorage.setItem(key, value); } catch { /* storage unavailable */ } }
function recall(key) { try { return localStorage.getItem(key) || ''; } catch { return ''; } }

/* ------------------------------------------------------------------ tabs, health, polling */

/** The URL hash keeps the view linkable: #tab=ops&order=<id>. */
function syncHash() {
    const params = new URLSearchParams();
    if (state.tab !== 'saga') params.set('tab', state.tab);
    if (state.orderId) params.set('order', state.orderId);
    history.replaceState(null, '', params.size ? `#${params}` : location.pathname);
}

function applyHash() {
    const params = new URLSearchParams(location.hash.slice(1));
    const order = params.get('order') || '';
    if (/^[0-9a-f-]{36}$/i.test(order) && order !== state.orderId) {
        state.orderId = order;
        state.mapKey = null;
    }
    const tab = params.get('tab');
    showTab(['saga', 'ops', 'data', 'console'].includes(tab) ? tab : 'saga');
}

function showTab(tab) {
    state.tab = tab;
    syncHash();
    document.querySelectorAll('.tabs button').forEach((b) => b.setAttribute('aria-selected', String(b.dataset.tab === tab)));
    document.querySelectorAll('main > .tab').forEach((s) => { s.hidden = s.id !== `tab-${tab}`; });
    if (tab === 'data') once('data', refreshData);
    if (tab === 'saga') once('options', loadOrderOptions);
    tick();
}

async function refreshHealth() {
    const results = await Promise.all(SERVICES.map((s) => get(s, '/actuator/health')));
    $('#health').replaceChildren(...SERVICES.map((s, i) => {
        const up = results[i].ok && results[i].data && results[i].data.status === 'UP';
        return h('li', { class: up ? 'up' : 'down', title: up ? `${s}-service is up` : `${s}-service is down or unreachable` }, s);
    }));
}

function tick() {
    if (document.hidden) return;
    if (state.ticks++ % 2 === 0) once('health', refreshHealth);
    if (state.tab === 'saga') {
        once('orders', refreshOrders);
        once('detail', refreshDetail);
    } else if (state.tab === 'ops') {
        once('ops', refreshOps);
    }
}

/* ------------------------------------------------------------------ saga tab: scenarios and ordering */

const SCENARIOS = [
    {
        key: 'happy', kind: 'fwd', title: 'Happy path', expect: 'Charged, reserved, approved',
        credit: 500, stock: 10, quantity: 2, amount: 120, count: 1,
        note: 'Payment is charged, stock is reserved and the order is approved.',
    },
    {
        key: 'credit', kind: 'fail', title: 'Insufficient credit', expect: 'Payment declined, order rejected',
        credit: 50, stock: 10, quantity: 1, amount: 200, count: 1,
        note: 'The payment is declined. Nothing was charged, so there is nothing to undo.',
    },
    {
        key: 'stock', kind: 'comp', title: 'Out of stock', expect: 'Charged, then refunded, order rejected',
        credit: 500, stock: 0, quantity: 1, amount: 80, count: 1,
        note: 'The payment succeeds, inventory fails, so the saga compensates with a refund. Credit ends where it started.',
    },
    {
        key: 'burst', kind: 'comp', title: 'Ten orders at once', expect: 'Five approved, five refunded',
        credit: 1000, stock: 50, quantity: 10, amount: 20, count: 10,
        note: 'Ten orders of 10 units compete for 50 in stock: five are approved, five are charged and then refunded.',
    },
];

function renderScenarios() {
    $('#scenarios').replaceChildren(...SCENARIOS.map((sc) => h('button', {
        type: 'button', class: 'scenario', 'data-kind': sc.kind, onclick: () => runScenario(sc),
    }, h('strong', { text: sc.title }), h('span', { text: sc.expect }))));
}

async function runScenario(sc) {
    const buttons = document.querySelectorAll('.scenario');
    const note = $('#scenario-note');
    buttons.forEach((b) => { b.disabled = true; });
    note.className = 'note';
    note.textContent = `Setting up "${sc.title}"…`;
    try {
        const suffix = Date.now().toString(36);
        const customerId = `${sc.key}-${suffix}`;
        const productId = `${sc.key}-${suffix}`;
        const customer = await call('payment', 'PUT', `/customers/${customerId}`, { availableCredit: sc.credit });
        if (!customer.ok) throw new Error(`Could not create the customer: ${errorText(customer)}`);
        const product = await call('inventory', 'PUT', `/products/${productId}`, { availableQuantity: sc.stock });
        if (!product.ok) throw new Error(`Could not create the product: ${errorText(product)}`);

        const body = { customerId, productId, quantity: sc.quantity, amount: sc.amount };
        const results = await Promise.all(Array.from({ length: sc.count }, () => call('order', 'POST', '/orders', body)));
        const placed = results.filter((r) => r.ok);
        if (placed.length === 0) throw new Error(`No order was placed: ${errorText(results[0])}`);

        note.textContent = `${sc.note} Customer ${customerId} started with ${money(sc.credit)} credit; the product had ${sc.stock} in stock.`;
        selectOrder(placed[0].data.id);
        once('options', loadOrderOptions);
    } catch (e) {
        note.className = 'error';
        note.textContent = e.message;
    } finally {
        buttons.forEach((b) => { b.disabled = false; });
    }
}

async function loadOrderOptions() {
    const [customers, products] = await Promise.all([get('payment', '/customers'), get('inventory', '/products')]);
    const form = $('#order-form');
    fillSelect(form.customerId, customers.ok ? customers.data.map((c) => [c.customerId, `${c.customerId} (credit ${money(c.availableCredit)})`]) : []);
    fillSelect(form.productId, products.ok ? products.data.map((p) => [p.productId, `${p.productId} (stock ${p.availableQuantity})`]) : []);
}

function fillSelect(select, options) {
    const current = select.value;
    select.replaceChildren(...options.map(([value, label]) => h('option', { value, text: label })));
    if (options.some(([value]) => value === current)) select.value = current;
}

async function placeOrder(event) {
    event.preventDefault();
    const form = event.target;
    const error = $('#order-error');
    error.textContent = '';
    const result = await call('order', 'POST', '/orders', {
        customerId: form.customerId.value,
        productId: form.productId.value,
        quantity: Number(form.quantity.value),
        amount: Number(form.amount.value),
    });
    if (!result.ok) {
        error.textContent = errorText(result);
        return;
    }
    selectOrder(result.data.id);
}

/* ------------------------------------------------------------------ saga tab: order list */

async function refreshOrders() {
    const list = $('#orders');
    const result = await get('order', '/orders?limit=30');
    if (!result.ok) {
        list.replaceChildren(h('li', { class: 'emptyrow', text: `Orders unavailable: ${errorText(result)}` }));
        return;
    }
    if (result.data.length === 0) {
        list.replaceChildren(h('li', { class: 'emptyrow', text: 'No orders yet. Run a scenario to create one.' }));
        return;
    }
    list.replaceChildren(...result.data.map((o) => h('li', {},
        h('button', { type: 'button', 'aria-current': String(o.id === state.orderId), onclick: () => selectOrder(o.id) },
            h('span', { class: 'oid', text: short(o.id) }),
            badge(o.status),
            h('span', { class: 'meta', text: `${o.quantity} × ${o.productId} for ${money(o.amount)}, ${human(o.sagaState)}, ${ago(o.createdAt)}` })))));
}

function selectOrder(orderId) {
    state.orderId = orderId;
    state.mapKey = null;
    if (state.tab !== 'saga') showTab('saga'); else syncHash();
    once('orders', refreshOrders);
    refreshDetail();
}

/* ------------------------------------------------------------------ saga tab: order detail */

async function refreshDetail() {
    const id = state.orderId;
    if (!id) return;
    const orderResult = await get('order', `/orders/${id}`);
    if (state.orderId !== id) return;
    if (!orderResult.ok) {
        $('#detail-empty').hidden = false;
        $('#detail-body').hidden = true;
        $('#detail-empty').firstElementChild.textContent = `Order ${short(id)} could not be loaded: ${errorText(orderResult)}`;
        return;
    }
    const order = orderResult.data;
    state.sagaId = order.sagaId;

    const [outOrder, outPayment, outInventory, payment, reservation, customer, product, saga] = await Promise.all([
        get('order', `/actuator/outbox/${id}`),
        get('payment', `/actuator/outbox/${id}`),
        get('inventory', `/actuator/outbox/${id}`),
        get('payment', `/payments/${id}`),
        get('inventory', `/reservations/${id}`),
        get('payment', `/customers/${encodeURIComponent(order.customerId)}`),
        get('inventory', `/products/${encodeURIComponent(order.productId)}`),
        get('order', `/actuator/stucksagas/${order.sagaId}`),
    ]);
    if (state.orderId !== id) return;

    const messages = [
        ...(outOrder.ok ? outOrder.data.map((m) => ({ ...m, service: 'order' })) : []),
        ...(outPayment.ok ? outPayment.data.map((m) => ({ ...m, service: 'payment' })) : []),
        ...(outInventory.ok ? outInventory.data.map((m) => ({ ...m, service: 'inventory' })) : []),
    ].sort((a, b) => Date.parse(a.createdAt) - Date.parse(b.createdAt));
    const sagaDetail = saga.ok ? saga.data : null;

    $('#detail-empty').hidden = true;
    $('#detail-body').hidden = false;
    renderHeader(order);
    renderMap(order, messages);
    renderTimeline(order, messages, sagaDetail);
    renderSaga(order, sagaDetail);
    renderParticipants(order, payment, reservation, customer, product);
}

function renderHeader(order) {
    $('#d-title').textContent = `Order ${order.id}`;
    $('#d-sub').textContent = `${order.customerId} buys ${order.quantity} × ${order.productId} for ${money(order.amount)}, placed ${ago(order.createdAt)}`;
    const status = $('#d-status');
    status.className = `badge ${order.status}`;
    status.textContent = human(order.status);
    const reason = $('#d-reason');
    reason.hidden = !order.rejectionReason;
    reason.textContent = order.rejectionReason ? `Rejected: ${order.rejectionReason}` : '';
}

/* The state machine as a transit map: stations are saga states, lit tracks are the route this order took. */

const STATIONS = {
    PAYMENT_PENDING: { x: 110, y: 50, label: 'Payment pending' },
    INVENTORY_PENDING: { x: 450, y: 50, label: 'Inventory pending' },
    COMPLETED: { x: 790, y: 50, label: 'Completed' },
    FAILED: { x: 110, y: 230, label: 'Failed' },
    COMPENSATING: { x: 450, y: 230, label: 'Refunding payment' },
    RELEASING_INVENTORY: { x: 790, y: 230, label: 'Releasing inventory' },
};
const NODE_W = 170;
const NODE_H = 42;

/* Label offsets are chosen so no two labels share space: verticals label outward, diagonals label away from them. */
const TRACKS = [
    { from: 'PAYMENT_PENDING', to: 'INVENTORY_PENDING', kind: 'fwd', label: 'PaymentProcessed', lx: 0, ly: -10 },
    { from: 'INVENTORY_PENDING', to: 'COMPLETED', kind: 'fwd', label: 'InventoryReserved', lx: 0, ly: -10 },
    { from: 'PAYMENT_PENDING', to: 'FAILED', kind: 'fwd', label: 'PaymentFailed', lx: -10, ly: 4, anchor: 'end' },
    { from: 'PAYMENT_PENDING', to: 'COMPENSATING', kind: 'comp', label: 'payment timeout', lx: -14, ly: 4, anchor: 'end' },
    { from: 'INVENTORY_PENDING', to: 'COMPENSATING', kind: 'comp', label: 'InventoryFailed', lx: 10, ly: 4, anchor: 'start' },
    { from: 'INVENTORY_PENDING', to: 'RELEASING_INVENTORY', kind: 'comp', label: 'inventory timeout', lx: 14, ly: 4, anchor: 'start' },
    { from: 'RELEASING_INVENTORY', to: 'COMPENSATING', kind: 'comp', label: 'InventoryReleased', lx: 0, ly: -10 },
    { from: 'COMPENSATING', to: 'FAILED', kind: 'comp', label: 'PaymentRefunded', lx: 0, ly: -10 },
];

/** Replays the order's commands to reconstruct the states it passed through. */
function derivePath(order, messages) {
    const route = ['PAYMENT_PENDING'];
    const move = (next) => { if (route[route.length - 1] !== next) route.push(next); };
    for (const m of messages) {
        const current = route[route.length - 1];
        if (m.messageType === 'ReserveInventory' && current === 'PAYMENT_PENDING') move('INVENTORY_PENDING');
        if (m.messageType === 'ReleaseInventory' && current === 'INVENTORY_PENDING') move('RELEASING_INVENTORY');
        if (m.messageType === 'RefundPayment' && current !== 'COMPENSATING') move('COMPENSATING');
    }
    move(order.sagaState);
    return route;
}

function renderMap(order, messages) {
    const route = derivePath(order, messages);
    const key = `${route.join('>')}|${order.sagaState}`;
    if (key === state.mapKey) return;
    state.mapKey = key;

    const travelled = new Set(route.slice(1).map((to, i) => `${route[i]}>${to}`));
    const visited = new Set(route);
    const map = $('#map');
    const title = map.querySelector('title');
    map.replaceChildren(title, markers());

    for (const t of TRACKS) {
        const on = travelled.has(`${t.from}>${t.to}`);
        const a = STATIONS[t.from];
        const b = STATIONS[t.to];
        const [x1, y1] = edgePoint(a, b.x - a.x, b.y - a.y, 0);
        const [x2, y2] = edgePoint(b, a.x - b.x, a.y - b.y, 7);
        map.append(svg('line', {
            x1, y1, x2, y2, class: `track ${on ? t.kind : ''}`, 'marker-end': `url(#arrow-${on ? t.kind : 'idle'})`,
        }));
        map.append(svg('text', {
            x: (x1 + x2) / 2 + t.lx, y: (y1 + y2) / 2 + t.ly, 'text-anchor': t.anchor || 'middle',
            class: `track-label ${on ? 'on' : ''}`,
        }, t.label));
    }

    const live = !['COMPLETED', 'FAILED'].includes(order.sagaState);
    for (const [name, s] of Object.entries(STATIONS)) {
        const classes = ['station'];
        if (visited.has(name)) classes.push('visited');
        if (visited.has(name) && name === 'COMPLETED') classes.push('ok');
        if (visited.has(name) && name === 'FAILED') classes.push('fail');
        if (name === order.sagaState) classes.push('current', live ? 'live' : '');
        const group = svg('g', { class: classes.join(' ').trim() });
        group.append(
            svg('rect', { x: s.x - NODE_W / 2, y: s.y - NODE_H / 2, width: NODE_W, height: NODE_H, rx: 6 }),
            svg('text', { x: s.x, y: s.y + 4, 'text-anchor': 'middle' }, s.label),
        );
        map.append(group);
    }
}

function markers() {
    const defs = svg('defs');
    for (const kind of ['idle', 'fwd', 'comp']) {
        const marker = svg('marker', {
            id: `arrow-${kind}`, viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 5, markerHeight: 5, orient: 'auto-start-reverse',
        });
        marker.append(svg('path', { d: 'M0,0 L10,5 L0,10 z', style: `fill: var(--${kind})` }));
        defs.append(marker);
    }
    return defs;
}

/** Where a line from a station's centre in direction (dx, dy) leaves its box, pushed out by `gap`. */
function edgePoint(station, dx, dy, gap) {
    const length = Math.hypot(dx, dy);
    const tx = dx === 0 ? Infinity : (NODE_W / 2) / Math.abs(dx);
    const ty = dy === 0 ? Infinity : (NODE_H / 2) / Math.abs(dy);
    const t = Math.min(tx, ty);
    return [station.x + dx * t + (dx / length) * gap, station.y + dy * t + (dy / length) * gap];
}

const COMPENSATION_TYPES = new Set(['RefundPayment', 'ReleaseInventory', 'PaymentRefunded', 'InventoryReleased']);
const FAILURE_TYPES = new Set(['PaymentFailed', 'InventoryFailed']);

function messageKind(type) {
    if (COMPENSATION_TYPES.has(type)) return 'comp';
    if (FAILURE_TYPES.has(type)) return 'fail';
    if (type === 'InventoryReserved') return 'ok';
    return '';
}

function renderTimeline(order, messages, sagaDetail) {
    const rows = messages.map((m) => ({ at: m.createdAt, lane: m.service, node: messageNode(m) }));
    for (const i of (sagaDetail && sagaDetail.interventions) || []) {
        rows.push({
            at: i.at,
            lane: 'order',
            node: h('div', { class: 'msg op' },
                h('strong', { text: `Operator ${human(i.action)} by ${i.operator}` }),
                h('span', { class: 'to', text: `${human(i.fromState)} to ${human(i.toState)}${i.note ? `: ${i.note}` : ''}` })),
        });
    }
    rows.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
    const list = $('#timeline');
    if (rows.length === 0) {
        list.replaceChildren(h('li', { class: 'emptyrow', text: 'No messages yet. The first command leaves within a second.' }));
        return;
    }
    const open = new Set([...list.querySelectorAll('details[open]')].map((d) => d.dataset.id));
    list.replaceChildren(...rows.map((r) => h('li', {},
        h('span', { class: 't', text: offset(r.at, order.createdAt) }),
        (() => { r.node.classList.add(`lane-${LANE[r.lane]}`); return r.node; })())));
    list.querySelectorAll('details').forEach((d) => { if (open.has(d.dataset.id)) d.open = true; });
}

function messageNode(m) {
    const target = TOPIC_TARGET[m.topic];
    const where = m.publishedAt ? `to ${target}` : `to ${target}, waiting in outbox`;
    return h('details', { class: `msg ${messageKind(m.messageType)}`, 'data-id': m.messageId },
        h('summary', { text: m.messageType }),
        h('span', { class: 'to', text: where }),
        h('pre', { text: JSON.stringify(m.payload, null, 2) }));
}

function renderSaga(order, saga) {
    const compensating = ['COMPENSATING', 'RELEASING_INVENTORY'].includes(order.sagaState);
    let deadline = '–';
    if (saga && saga.deadline) {
        const seconds = (Date.parse(saga.deadline) - Date.now()) / 1000;
        deadline = seconds > 0 ? `times out in ${duration(seconds)}` : 'overdue, timing out now';
    }
    facts($('#saga-facts'), [
        ['State', badge(order.sagaState)],
        ['Deadline', deadline],
        ['Re-sends', saga ? String(saga.compensationResends) : '–'],
        ['Compensation started', saga && saga.compensatingSince ? ago(saga.compensatingSince) : 'never'],
        ['Saga id', h('code', { text: short(order.sagaId), title: order.sagaId })],
    ]);
    $('#intervene').hidden = !compensating;
    $('#history').replaceChildren(...((saga && saga.interventions) || []).map((i) => h('li', {},
        `${human(i.action)} by ${i.operator}, ${ago(i.at)}${i.note ? `: ${i.note}` : ''}`)));
}

function renderParticipants(order, payment, reservation, customer, product) {
    facts($('#payment-facts'), [
        ['Payment', payment.ok ? badge(payment.data.status) : (payment.status === 404 ? 'no charge recorded' : 'unavailable')],
        ['Amount', payment.ok && payment.data.amount !== null ? money(payment.data.amount) : '–'],
        ['Credit now', customer.ok ? money(customer.data.availableCredit) : 'unavailable'],
    ]);
    facts($('#inventory-facts'), [
        ['Reservation', reservation.ok ? badge(reservation.data.status) : (reservation.status === 404 ? 'nothing reserved' : 'unavailable')],
        ['Quantity', reservation.ok ? String(reservation.data.quantity) : '–'],
        ['Stock now', product.ok ? String(product.data.availableQuantity) : 'unavailable'],
    ]);
}

async function intervene(event) {
    event.preventDefault();
    const form = event.target;
    const action = event.submitter ? event.submitter.value : 'retry';
    const error = $('#intervene-error');
    error.textContent = '';
    store('operator', form.operator.value);
    const result = await call('order', 'POST', `/actuator/stucksagas/${state.sagaId}`, {
        action, operator: form.operator.value, note: form.note.value || null,
    });
    if (!result.ok) {
        error.textContent = errorText(result);
        return;
    }
    form.note.value = '';
    refreshDetail();
}

/* ------------------------------------------------------------------ operations tab */

const METRICS = [
    { name: 'saga.compensation.stuck', label: 'Stuck sagas', alert: (v) => v > 0, format: String },
    { name: 'saga.compensation.in.progress', label: 'Compensating now', format: String },
    { name: 'saga.compensation.oldest.age', label: 'Oldest compensation', format: (v) => (v > 0 ? duration(v) : '–') },
    { name: 'saga.compensation.resends', label: 'Compensation re-sends', format: String },
];

async function refreshOps() {
    const [metricResults, stuck, dlts] = await Promise.all([
        Promise.all(METRICS.map((m) => get('order', `/actuator/metrics/${m.name}`))),
        get('order', '/actuator/stucksagas'),
        Promise.all(SERVICES.map((s) => get(s, '/actuator/dlt'))),
    ]);

    $('#tiles').replaceChildren(...METRICS.map((m, i) => {
        const r = metricResults[i];
        const value = r.ok ? r.data.measurements[0].value : null;
        return h('div', { class: `tile ${value !== null && m.alert && m.alert(value) ? 'alert' : ''}` },
            h('div', { class: 'v', text: value === null ? '–' : m.format(value) }),
            h('div', { class: 'l', text: m.label }));
    }));

    const table = $('#stuck');
    const head = h('tr', {}, ...['Order', 'State', 'Reason', 'Re-sends', 'Compensating since', ''].map((t) => h('th', { text: t })));
    if (!stuck.ok) {
        table.replaceChildren(head, h('tr', { class: 'emptyrow' }, h('td', { colspan: 6, text: `Unavailable: ${errorText(stuck)}` })));
    } else if (stuck.data.length === 0) {
        table.replaceChildren(head, h('tr', { class: 'emptyrow' }, h('td', { colspan: 6, text: 'No stuck sagas.' })));
    } else {
        table.replaceChildren(head, ...stuck.data.map((s) => h('tr', {},
            h('td', {}, h('code', { text: short(s.orderId) })),
            h('td', {}, badge(s.state)),
            h('td', { text: s.failureReason || '' }),
            h('td', { class: 'num', text: String(s.compensationResends) }),
            h('td', { text: ago(s.compensatingSince) }),
            h('td', {}, h('button', { type: 'button', onclick: () => selectOrder(s.orderId), text: 'Open order' })))));
    }

    renderDlts(dlts);
}

const dltCards = new Map();

function renderDlts(results) {
    const container = $('#dlts');
    SERVICES.forEach((service, i) => {
        const r = results[i];
        const entries = r.ok ? r.data : [{ topic: null, dltTopic: `${service}-service unreachable`, pending: null }];
        for (const entry of entries) {
            const key = `${service}|${entry.dltTopic}`;
            let card = dltCards.get(key);
            if (!card) {
                card = dltCard(service, entry);
                dltCards.set(key, card);
                container.append(card.root);
            }
            card.pending.textContent = entry.pending === null ? '–' : `${entry.pending} pending`;
            card.pending.className = `pending ${entry.pending > 0 ? 'has' : ''}`;
        }
    });
}

function dltCard(service, entry) {
    const pending = h('div', { class: 'pending' });
    const result = h('p', { class: 'result', 'aria-live': 'polite' });
    const limit = h('input', { type: 'number', min: 1, max: 10000, value: 100, 'aria-label': 'Replay limit' });
    const button = h('button', { type: 'submit', text: 'Replay', disabled: entry.topic === null });
    const form = h('form', {
        onsubmit: async (event) => {
            event.preventDefault();
            button.disabled = true;
            result.textContent = 'Replaying…';
            const r = await call(service, 'POST', `/actuator/dlt/${entry.topic}`, { limit: Number(limit.value) });
            button.disabled = false;
            result.textContent = r.ok ? `Replayed ${r.data.replayed}; ${r.data.pending} still pending.` : errorText(r);
            once('ops', refreshOps);
        },
    }, limit, button);
    const root = h('div', { class: 'dlt' },
        h('div', { class: 'topic', text: entry.dltTopic }),
        h('div', { class: 'l', text: `${service}-service` }),
        pending, form, result);
    return { root, pending };
}

const PRESETS = [
    {
        label: 'Unknown message type', topic: 'payment.commands', type: 'NoSuchCommand',
        payload: () => '{"foo": 1}',
    },
    {
        label: 'Charge that crashes the handler', topic: 'payment.commands', type: 'ProcessPayment',
        payload: () => JSON.stringify({ sagaId: uuid(), orderId: uuid(), customerId: 'customer-1', amount: null }, null, 2),
    },
    {
        label: 'Reply for a saga that does not exist', topic: 'order.saga.replies', type: 'PaymentProcessed',
        payload: () => JSON.stringify({ sagaId: uuid(), orderId: uuid() }, null, 2),
    },
    {
        label: 'Malformed JSON', topic: 'inventory.commands', type: 'ReserveInventory',
        payload: () => '{not json',
    },
];

function renderPresets() {
    const form = $('#inject-form');
    $('#presets').replaceChildren(...PRESETS.map((p) => h('button', {
        type: 'button',
        onclick: () => {
            form.topic.value = p.topic;
            form.messageType.value = p.type;
            form.payload.value = p.payload();
            form.key.value = '';
            form.partition.value = '';
            form.count.value = 1;
        },
    }, p.label)));
}

async function inject(event) {
    event.preventDefault();
    const form = event.target;
    const out = $('#inject-out');
    const result = await call('ui', 'POST', '/api/inject', {
        topic: form.topic.value,
        messageType: form.messageType.value,
        key: form.key.value || null,
        partition: form.partition.value === '' ? null : Number(form.partition.value),
        count: Number(form.count.value || 1),
        payload: form.payload.value,
    });
    out.hidden = false;
    out.textContent = result.ok
        ? `Published ${result.data.messageIds.length} message(s) to ${result.data.topic}. Watch the dead-letter counts below.\n\n${result.data.messageIds.join('\n')}`
        : errorText(result);
}

/* ------------------------------------------------------------------ customers & products tab */

async function refreshData() {
    const [customers, products] = await Promise.all([get('payment', '/customers'), get('inventory', '/products')]);
    dataTable($('#customers'), customers, ['Customer', 'Credit'],
        (c) => [c.customerId, money(c.availableCredit)], (c) => fillForm('#customer-form', c.customerId, c.availableCredit));
    dataTable($('#products'), products, ['Product', 'Stock'],
        (p) => [p.productId, String(p.availableQuantity)], (p) => fillForm('#product-form', p.productId, p.availableQuantity));
}

function dataTable(table, result, headers, cells, onPick) {
    const head = h('tr', {}, h('th', { text: headers[0] }), h('th', { class: 'num', text: headers[1] }), h('th'));
    if (!result.ok) {
        table.replaceChildren(head, h('tr', { class: 'emptyrow' }, h('td', { colspan: 3, text: `Unavailable: ${errorText(result)}` })));
        return;
    }
    table.replaceChildren(head, ...result.data.map((row) => {
        const [id, value] = cells(row);
        return h('tr', {}, h('td', {}, h('code', { text: id })), h('td', { class: 'num', text: value }),
            h('td', {}, h('button', { type: 'button', onclick: () => onPick(row), text: 'Edit' })));
    }));
}

function fillForm(selector, id, value) {
    const form = $(selector);
    form.id.value = id;
    form.value.value = value;
    form.value.focus();
}

function saveData(service, collection, field, errorSelector) {
    return async (event) => {
        event.preventDefault();
        const form = event.target;
        const error = $(errorSelector);
        error.textContent = '';
        const value = Number(form.value.value);
        const result = await call(service, 'PUT', `/${collection}/${encodeURIComponent(form.id.value)}`, { [field]: value });
        if (!result.ok) {
            error.textContent = errorText(result);
            return;
        }
        refreshData();
        loadOrderOptions();
    };
}

/* ------------------------------------------------------------------ API console */

const CATALOGUE = {
    order: [
        ['POST', '/orders', 'Place an order', { customerId: 'customer-1', productId: 'product-1', quantity: 1, amount: 25.0 }],
        ['GET', '/orders?limit=10', 'Recent orders'],
        ['GET', '/orders/{orderId}', 'One order with its saga state'],
        ['GET', '/actuator/outbox/{orderId}', 'Commands sent for an order'],
        ['GET', '/actuator/stucksagas', 'Stuck sagas'],
        ['GET', '/actuator/stucksagas/{sagaId}', 'Saga detail and interventions'],
        ['POST', '/actuator/stucksagas/{sagaId}', 'Retry a compensation', { action: 'retry', operator: 'me' }],
        ['POST', '/actuator/stucksagas/{sagaId}', 'Resolve a compensation', { action: 'resolve', operator: 'me', note: 'Refunded in payment_db' }],
        ['GET', '/actuator/dlt', 'Dead-letter topics'],
        ['POST', '/actuator/dlt/order.saga.replies', 'Replay dead-lettered replies', { limit: 100 }],
        ['GET', '/actuator/metrics/saga.compensation.stuck', 'Stuck gauge'],
        ['GET', '/actuator/metrics', 'All metric names'],
        ['GET', '/actuator/prometheus', 'Prometheus scrape'],
        ['GET', '/actuator/health', 'Health'],
    ],
    payment: [
        ['GET', '/customers', 'All customers'],
        ['GET', '/customers/{customerId}', 'One customer'],
        ['PUT', '/customers/{customerId}', 'Set credit', { availableCredit: 1000.0 }],
        ['GET', '/payments/{orderId}', 'Payment for an order'],
        ['GET', '/actuator/outbox/{orderId}', 'Replies sent for an order'],
        ['GET', '/actuator/dlt', 'Dead-letter topics'],
        ['POST', '/actuator/dlt/payment.commands', 'Replay dead-lettered commands', { limit: 100 }],
        ['GET', '/actuator/health', 'Health'],
    ],
    inventory: [
        ['GET', '/products', 'All products'],
        ['GET', '/products/{productId}', 'One product'],
        ['PUT', '/products/{productId}', 'Set stock', { availableQuantity: 100 }],
        ['GET', '/reservations/{orderId}', 'Reservation for an order'],
        ['GET', '/actuator/outbox/{orderId}', 'Replies sent for an order'],
        ['GET', '/actuator/dlt', 'Dead-letter topics'],
        ['POST', '/actuator/dlt/inventory.commands', 'Replay dead-lettered commands', { limit: 100 }],
        ['GET', '/actuator/health', 'Health'],
    ],
    ui: [
        ['POST', '/api/inject', 'Publish a raw message', { topic: 'payment.commands', messageType: 'NoSuchCommand', payload: '{"foo":1}' }],
        ['GET', '/actuator/health', 'Console health'],
    ],
};

const SERVICE_LABEL = { order: 'order-service', payment: 'payment-service', inventory: 'inventory-service', ui: 'This console' };

function renderCatalogue() {
    const aside = $('#catalogue');
    for (const [service, endpoints] of Object.entries(CATALOGUE)) {
        aside.append(h('h3', { text: SERVICE_LABEL[service] }));
        for (const [method, path, description, body] of endpoints) {
            aside.append(h('button', { type: 'button', onclick: () => loadRequest(service, method, path, body) },
                h('span', { class: `m ${method}`, text: method }),
                h('span', { class: 'p', text: path }),
                h('span', { class: 'd', text: description })));
        }
    }
}

function loadRequest(service, method, path, body) {
    const form = $('#console-form');
    form.service.value = service;
    form.method.value = method;
    form.path.value = path
        .replace('{orderId}', state.orderId || '00000000-0000-0000-0000-000000000000')
        .replace('{sagaId}', state.sagaId || '00000000-0000-0000-0000-000000000000')
        .replace('{customerId}', 'customer-1')
        .replace('{productId}', 'product-1');
    form.body.value = body ? JSON.stringify(body, null, 2) : '';
    form.path.focus();
}

async function sendConsole(event) {
    event.preventDefault();
    const form = event.target;
    const button = form.querySelector('button[type="submit"]');
    button.disabled = true;
    const path = form.path.value.startsWith('/') ? form.path.value : `/${form.path.value}`;
    const result = await call(form.service.value, form.method.value, path, form.body.value.trim() || undefined);
    button.disabled = false;
    const meta = $('#console-meta');
    meta.replaceChildren(
        h('span', { class: `s${String(result.status)[0]}`, text: result.status === 0 ? 'No response' : `HTTP ${result.status}` }),
        ` in ${result.ms} ms`);
    $('#console-out').hidden = false;
    $('#console-out').textContent = typeof result.data === 'string' || result.data === null
        ? result.text
        : JSON.stringify(result.data, null, 2);
}

/* ------------------------------------------------------------------ start */

function init() {
    document.querySelectorAll('.tabs button').forEach((b) => b.addEventListener('click', () => showTab(b.dataset.tab)));
    $('#order-form').addEventListener('submit', placeOrder);
    $('#intervene-form').addEventListener('submit', intervene);
    $('#intervene-form').operator.value = recall('operator');
    $('#inject-form').addEventListener('submit', inject);
    $('#customer-form').addEventListener('submit', saveData('payment', 'customers', 'availableCredit', '#customer-error'));
    $('#product-form').addEventListener('submit', saveData('inventory', 'products', 'availableQuantity', '#product-error'));
    $('#console-form').addEventListener('submit', sendConsole);

    renderScenarios();
    renderPresets();
    renderCatalogue();
    $('#presets').firstElementChild.click();

    applyHash();
    window.addEventListener('hashchange', applyHash);

    loadOrderOptions();
    setInterval(tick, POLL_MS);
    document.addEventListener('visibilitychange', tick);
}

init();
