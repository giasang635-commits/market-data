"""Исторический бэктест (только чтение публичных данных, никакой торговли).
Вопрос: в сентябре 2026 после пампов бывают резкие залпы продаж?
Если да — сколько раз и чем закончился бы шорт после залпа (TP 1% / SL 1.5%).
Данные: 1h-свечи из этого репо + публичный архив сделок Bybit (public.bybit.com/trading).
Результат: data/kit_surf/sep_triggers_white.csv (старые монеты) и sep_triggers_rest.csv (остальные).
Запуск: REPO=. PROXY=<proxy или пусто> MODE=<white|rest|both> python3 -u scripts/kit_surf_sep.py
v2: прогресс в лог, результат дописывается в CSV после каждого дня, огромные дневные файлы (крупные монеты) пропускаются.
"""
import glob, io, json, os, re, urllib.request
import numpy as np, pandas as pd

REPO = os.environ.get('REPO', '.')
PROXY = os.environ.get('PROXY', '')
DAY_FROM, DAY_TO = '2026-09-15', '2026-10-01'
BURST, DROP = 40, 0.01          # продажи за 2 с >= 40x нормы и цена -1% за 2 с
DELAY, TP, SL, FEE = 1.0, 0.01, 0.015, 0.0011
HORIZON, COOLDOWN = 600, 1800   # секунды
MODE = os.environ.get('MODE', 'both')
MAX_GZ_MB = 60                  # дневной архив больше — пропуск (крупные монеты, не наш тип)

handlers = [urllib.request.ProxyHandler({'https': PROXY})] if PROXY else []
opener = urllib.request.build_opener(*handlers)

old = set()
for line in open(f'{REPO}/data/kit/kit_raw.jsonl'):
    m = re.search(r'PUMP DETECTED[:\s]+([A-Z0-9]+)', json.loads(line).get('text') or '', re.I)
    if m:
        old.add(m.group(1).upper())
every = {os.path.basename(p).split('-USDT')[0] + 'USDT'
         for p in glob.glob(f'{REPO}/data/bybit/*-USDT-PERP_1h.parquet')} - {'BTCUSDT', 'ETHUSDT'}

def pump_hours(sym):
    """Часы, когда монета +10% за 24ч и оборот за 24ч >= $3M (листинги 2026 года пропускаем)."""
    p = f'{REPO}/data/bybit/{sym[:-4]}-USDT-PERP_1h.parquet'
    if not os.path.exists(p):
        return []
    c = pd.read_parquet(p)
    c.index = pd.to_datetime(c.ts, unit='ms', utc=True)
    c = c[~c.index.duplicated()].sort_index()
    if c.index[0] > pd.Timestamp('2026-01-02', tz='UTC'):
        return []
    ok = (c.close / c.close.shift(24) - 1 >= 0.10) & ((c.volume * c.close).rolling(24).sum() >= 3e6)
    return [t + pd.Timedelta('1h') for t in c.index[ok] if DAY_FROM <= str(t.date()) < DAY_TO]

def scan_day(trades, hours, sym):
    trades = trades.sort_values('timestamp')
    trades['sec'] = np.floor(trades.timestamp).astype(int)
    sec = np.arange(trades.sec.min(), trades.sec.max() + 1)
    sells = trades[trades.side == 'Sell'].groupby('sec').foreignNotional.sum().reindex(sec, fill_value=0).values
    last_px = trades.groupby('sec').price.last().reindex(sec).ffill().values
    active = np.zeros(len(sec), bool)
    for h in hours:
        a = int(h.timestamp())
        active |= (sec >= a) & (sec < a + 3600)
    ts, px = trades.timestamp.values, trades.price.values
    found, last_hit = [], -10**12
    for i in range(300, len(sec) - 1):
        if not active[i] or sec[i] - last_hit < COOLDOWN:
            continue
        norm = sells[i - 300:i - 2].mean() + 1e-9
        now = sells[i - 1:i + 1].sum() / 2
        if now >= BURST * norm and last_px[i] <= last_px[i - 2] * (1 - DROP):
            t_in = sec[i] + 1 + DELAY
            j = np.searchsorted(ts, t_in)
            if j >= len(ts):
                continue
            entry = px[j]
            w = (ts >= t_in) & (ts <= t_in + HORIZON)
            wp, wt = px[w], ts[w]
            tp_hit = np.flatnonzero(wp <= entry * (1 - TP))
            sl_hit = np.flatnonzero(wp >= entry * (1 + SL))
            if len(tp_hit) and (not len(sl_hit) or tp_hit[0] < sl_hit[0]):
                res, hold = TP - FEE, wt[tp_hit[0]] - t_in
            elif len(sl_hit):
                res, hold = -SL - FEE - 0.001, wt[sl_hit[0]] - t_in
            else:
                res, hold = (1 - wp[-1] / entry - FEE) if len(wp) else 0, HORIZON
            found.append(dict(sym=sym, t=pd.Timestamp(sec[i], unit='s', tz='UTC'), burst=round(now / norm, 1),
                              entry=entry, res=res, hold_s=round(hold, 1)))
            last_hit = sec[i]
    return found

os.makedirs(f'{REPO}/data/kit_surf', exist_ok=True)
for name, coins in (('white', old), ('rest', every - old)):
    if MODE not in (name, 'both'):
        continue
    out = f'{REPO}/data/kit_surf/sep_triggers_{name}.csv'
    if os.path.exists(out):
        os.remove(out)
    total, days, missing, skipped = 0, 0, 0, 0
    for sym in sorted(coins):
        hours = pump_hours(sym)
        for day in sorted({h.date() for h in hours}):
            url = f'https://public.bybit.com/trading/{sym}/{sym}{day}.csv.gz'
            try:
                raw = opener.open(url, timeout=120).read()
            except Exception:
                missing += 1
                continue
            if len(raw) > MAX_GZ_MB * 1e6:
                skipped += 1
                print(f'[{name}] пропуск {sym} {day}: архив {len(raw) / 1e6:.0f} МБ', flush=True)
                continue
            days += 1
            trades = pd.read_csv(io.BytesIO(raw), compression='gzip',
                                 usecols=['timestamp', 'side', 'price', 'foreignNotional'])
            del raw
            found = scan_day(trades, [h for h in hours if h.date() == day or (h + pd.Timedelta('1h')).date() == day], sym)
            del trades
            if found:
                pd.DataFrame(found).to_csv(out, mode='a', header=not os.path.exists(out), index=False)
                total += len(found)
            print(f'[{name}] {sym} {day}: срабатываний {len(found)}, всего {total}', flush=True)
    print(f'[{name}] ИТОГ: монето-дней {days}, нет в архиве {missing}, пропущено крупных {skipped}, срабатываний {total}', flush=True)
    if total:
        R = pd.read_csv(out)
        print(f'[{name}] ИТОГ: в плюсе {(R.res > 0).mean() * 100:.0f}% | средний {R.res.mean() * 100:.2f}% | в день {len(R) / 16:.1f}', flush=True)
