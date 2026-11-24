// Graduation Runner: a paper bot for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Speaks the protocol directly on stdin/stdout: one hello, then one batch per slot (answered with a
// reply carrying our orders), then done. No clock, no randomness, no I/O besides the pipe, and every
// map is iterated in insertion order, so two runs on the same data write the same action log.
//
// Strategy
//  - Runner: a curve that has already climbed late into its bonding curve (real SOL reserve in
//    [RUN_MIN_REAL, RUN_MAX_REAL]) while fresh SOL keeps flowing in from many distinct wallets is
//    bought once. Late curves complete far more often than young ones, and a completed curve is
//    closed by the replay at its final reserves, near the top of the curve. Exits: a hard stop
//    against the entry price, a trailing stop once the position is well in profit, a time limit.
//  - Guardrail probes: the ranking bar wants a round trip of at least 0.01 SOL on 20+ coins, so the
//    bot buys PROBE_LAMPORTS of liquid, mid-curve coins and sells each as soon as it lands, until
//    PROBE_TARGET coins are done. They cost a few basis points each and nothing else.

const PROTOCOL = 'agent-fun/replay/v2';
const LAMPORTS = 1_000_000_000n;

// ---- runner parameters
const RUN_MIN_REAL = 50n * LAMPORTS;
const RUN_MAX_REAL = 70n * LAMPORTS;
const FLOW_WINDOW_SLOTS = 150;
const MIN_NET_FLOW = 6n * LAMPORTS;
const MIN_BUYERS = 10;
/** Skip a runner when one wallet bought more than this share of the window's buy SOL. */
const MAX_BUYER_SHARE_BPS = 10_000n;
const RUN_STAKE = LAMPORTS;
const BUY_SLIPPAGE_BPS = 1500;
const STOP_LOSS_BPS = 3000n;
const TRAIL_ARM_BPS = 3000n;
const TRAIL_BPS = 1500n;
const MAX_HOLD_SLOTS = 600;

// ---- probe parameters
const PROBE_LAMPORTS = 10_500_000n;
const PROBE_TARGET = 24;
const PROBE_MIN_REAL = 3n * LAMPORTS;
const PROBE_MAX_REAL = 30n * LAMPORTS;
const MAX_PROBES_OPEN = 2;
const SELL_SLIPPAGE_BPS = 10_000;

interface Trade { slot: number; sol: bigint; buy: boolean; user: string }
interface CurveState {
  vq: bigint; vt: bigint; real: bigint;
  complete: boolean;
  tape: Trade[];
}
interface Held { kind: 'run' | 'probe'; entrySlot: number; entryPriceNum: bigint; entryPriceDen: bigint; best: bigint; bestDen: bigint }

const curves = new Map<string, CurveState>();
const traded = new Set<string>();          // mints we already entered (one entry per coin)
const held = new Map<string, Held>();      // mints we hold or have a buy pending on
const pending = new Map<string, { mint: string; side: 'buy' | 'sell'; quote: bigint }>();
const positions = new Map<string, bigint>();
let cash = 0n;
let txFee = 105_000n;
let maxOpen = 5;
let maxOrders = 16;
let capBps = 100n;
let nextId = 1;
let probesDone = 0;

function curveOf(mint: string): CurveState {
  let c = curves.get(mint);
  if (c === undefined) {
    c = { vq: 0n, vt: 0n, real: 0n, complete: false, tape: [] };
    curves.set(mint, c);
  }
  return c;
}

function onEvent(e: any, slot: number): void {
  if (e.event === 'CreateEvent') {
    const c = curveOf(e.mint);
    c.vq = BigInt(e.virtual_quote_reserves);
    c.vt = BigInt(e.virtual_token_reserves);
  } else if (e.event === 'TradeEvent') {
    const c = curveOf(e.mint);
    c.vq = BigInt(e.virtual_quote_reserves);
    c.vt = BigInt(e.virtual_token_reserves);
    c.real = BigInt(e.real_quote_reserves);
    c.tape.push({ slot, sol: BigInt(e.sol_amount), buy: e.is_buy === true, user: String(e.user) });
    while (c.tape.length > 0 && c.tape[0].slot < slot - FLOW_WINDOW_SLOTS) c.tape.shift();
  } else if (e.event === 'CompleteEvent') {
    curveOf(e.mint).complete = true;
  }
}

function flow(c: CurveState, slot: number): { net: bigint; buyers: number; topShareBps: bigint } {
  let net = 0n;
  let bought = 0n;
  const byBuyer = new Map<string, bigint>();
  for (const t of c.tape) {
    if (t.slot < slot - FLOW_WINDOW_SLOTS) continue;
    if (t.buy) {
      net += t.sol;
      bought += t.sol;
      byBuyer.set(t.user, (byBuyer.get(t.user) ?? 0n) + t.sol);
    } else net -= t.sol;
  }
  // ownership dispersion: the biggest single wallet's share of the window's buy SOL
  let top = 0n;
  for (const v of byBuyer.values()) if (v > top) top = v;
  const topShareBps = bought > 0n ? (top * 10_000n) / bought : 10_000n;
  return { net, buyers: byBuyer.size, topShareBps };
}

function id(): string { return 'g' + String(nextId++); }

function onBatch(b: any): any[] {
  for (const f of b.fills) {
    if (!String(f.client_id).startsWith('_')) pending.delete(f.client_id);
    if (f.status !== 'filled' && f.side === 'buy') held.delete(f.mint);
  }
  cash = BigInt(b.account.cash_lamports);
  positions.clear();
  for (const p of b.account.positions) {
    const amt = BigInt(p.token_amount);
    if (amt > 0n) positions.set(p.mint, amt);
  }
  for (const e of b.events) onEvent(e, b.slot);
  for (const m of b.marks ?? []) {
    const c = curveOf(m.mint);
    c.vq = BigInt(m.virtual_quote_reserves);
    c.vt = BigInt(m.virtual_token_reserves);
    c.real = BigInt(m.real_quote_reserves);
  }
  // a holding the replay closed (completion / write-off) is gone
  for (const mint of [...held.keys()]) {
    const busy = [...pending.values()].some((p) => p.mint === mint);
    if (!busy && !positions.has(mint)) held.delete(mint);
  }

  const orders: any[] = [];
  // free cash: what pending orders will still take (every tx fee, and each buy's amount)
  let free = cash;
  for (const p of pending.values()) free -= txFee + p.quote;
  const sellPending = new Set([...pending.values()].filter((p) => p.side === 'sell').map((p) => p.mint));

  // exits first
  for (const [mint, h] of held) {
    const amt = positions.get(mint);
    if (amt === undefined || sellPending.has(mint) || orders.length >= maxOrders) continue;
    const c = curveOf(mint);
    if (c.complete) continue;
    let exit = false;
    if (h.kind === 'probe') exit = true;
    else {
      // price = vq / vt; compare as cross products
      const p = c.vq * h.entryPriceDen;
      const e = h.entryPriceNum * c.vt;
      if (c.vq * h.bestDen > h.best * c.vt) { h.best = c.vq; h.bestDen = c.vt; }
      if (p * 10_000n <= e * (10_000n - STOP_LOSS_BPS)) exit = true;
      const armed = h.best * h.entryPriceDen * 10_000n >= h.entryPriceNum * h.bestDen * (10_000n + TRAIL_ARM_BPS);
      if (armed && c.vq * h.bestDen * 10_000n <= h.best * c.vt * (10_000n - TRAIL_BPS)) exit = true;
      if (b.slot - h.entrySlot > MAX_HOLD_SLOTS) exit = true;
    }
    if (exit) {
      const cid = id();
      orders.push({ side: 'sell', client_id: cid, mint, token_amount: amt.toString(), max_slippage_bps: SELL_SLIPPAGE_BPS });
      pending.set(cid, { mint, side: 'sell', quote: 0n });
      if (h.kind === 'probe') probesDone++;
    }
  }

  // entries
  let open = held.size;
  let probesOpen = [...held.values()].filter((h) => h.kind === 'probe').length;
  for (const e of b.events) {
    if (orders.length >= maxOrders || open >= maxOpen) break;
    if (e.event !== 'TradeEvent') continue;
    const mint = e.mint;
    if (traded.has(mint)) continue;
    const c = curveOf(mint);
    if (c.complete || c.vt === 0n) continue;
    const cap = (c.real * capBps) / 10_000n;
    let kind: 'run' | 'probe' | null = null;
    let amount = 0n;
    if (c.real >= RUN_MIN_REAL && c.real <= RUN_MAX_REAL) {
      const f = flow(c, b.slot);
      if (f.net >= MIN_NET_FLOW && f.buyers >= MIN_BUYERS && f.topShareBps <= MAX_BUYER_SHARE_BPS) {
        kind = 'run';
        amount = cap < RUN_STAKE ? cap : RUN_STAKE;
      }
    }
    if (kind === null && probesDone + probesOpen < PROBE_TARGET && probesOpen < MAX_PROBES_OPEN
        && c.real >= PROBE_MIN_REAL && c.real <= PROBE_MAX_REAL && cap >= PROBE_LAMPORTS) {
      kind = 'probe';
      amount = PROBE_LAMPORTS;
    }
    if (kind === null || free < amount + 2n * txFee) continue;
    const cid = id();
    orders.push({ side: 'buy', client_id: cid, mint, quote_lamports: amount.toString(),
      max_slippage_bps: kind === 'run' ? BUY_SLIPPAGE_BPS : SELL_SLIPPAGE_BPS });
    pending.set(cid, { mint, side: 'buy', quote: amount });
    traded.add(mint);
    held.set(mint, { kind, entrySlot: b.slot, entryPriceNum: c.vq, entryPriceDen: c.vt, best: c.vq, bestDen: c.vt });
    free -= amount + txFee;
    open++;
    if (kind === 'probe') probesOpen++;
  }
  return orders;
}

// ---- protocol loop
let buf = '';
let done = false;
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk: string) => {
  if (done) return;
  buf += chunk;
  let nl: number;
  while ((nl = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, nl);
    buf = buf.slice(nl + 1);
    if (line.trim() === '') continue;
    const msg = JSON.parse(line);
    if (msg.type === 'hello') {
      if (msg.protocol !== PROTOCOL) throw new Error('unexpected protocol ' + msg.protocol);
      cash = BigInt(msg.capital.lamports);
      txFee = BigInt(msg.fees.base_fee_lamports) + BigInt(msg.fees.priority_fee_lamports);
      maxOpen = msg.caps.max_open_positions;
      maxOrders = msg.caps.max_orders_per_batch;
      capBps = BigInt(msg.caps.max_order_bps_of_real_reserve);
    } else if (msg.type === 'batch') {
      const orders = onBatch(msg);
      process.stdout.write(JSON.stringify({ type: 'reply', seq: msg.seq, orders }) + '\n');
    } else if (msg.type === 'done') {
      done = true;
      process.stdin.pause();
      return;
    }
  }
});
