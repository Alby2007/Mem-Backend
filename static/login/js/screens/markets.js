// ── MARKETS ──────────────────────────────────────────────────────────────────
let _tvCurrentSym  = 'FOREXCOM:SPXUSD';
let _tvCurrentKb   = 'SPX';   // KB ticker for the currently shown symbol
let _marketsCat    = 'indices';
let _marketsInited = false;
window._tvWidgetInstance = null;

const _MARKET_CATS = {
  indices: [
    { label: 'S&P 500',    sym: 'FOREXCOM:SPXUSD', kb: 'SPX'    },
    { label: 'NASDAQ 100', sym: 'FOREXCOM:NSXUSD',  kb: 'NDX'    },
    { label: 'DOW 30',     sym: 'FOREXCOM:DJI',     kb: 'DJI'    },
    { label: 'FTSE 100',   sym: 'SPREADEX:FTSE',    kb: 'FTSE'   },
    { label: 'DAX 40',     sym: 'XETR:DAX',         kb: 'DAX'    },
    { label: 'CAC 40',     sym: 'EURONEXT:PX1',     kb: 'CAC'    },
    { label: 'Nikkei 225', sym: 'TVC:NI225',        kb: 'NI225'  },
    { label: 'Hang Seng',  sym: 'TVC:HSI',          kb: 'HSI'    },
    { label: 'ASX 200',    sym: 'TVC:ASX',          kb: 'ASX'    },
    { label: 'Euro Stoxx', sym: 'TVC:SX5E',         kb: 'SX5E'   },
    { label: 'VIX',        sym: 'TVC:VIX',          kb: 'VIX'    },
    { label: 'FTSE 250',   sym: 'TVC:MCX',          kb: 'MCX'    },
  ],
  uk_equities: [
    { label: 'BARC',  sym: 'LSE:BARC',  kb: 'BARC.L'  },
    { label: 'HSBA',  sym: 'LSE:HSBA',  kb: 'HSBA.L'  },
    { label: 'LLOY',  sym: 'LSE:LLOY',  kb: 'LLOY.L'  },
    { label: 'LSEG',  sym: 'LSE:LSEG',  kb: 'LSEG.L'  },
    { label: 'NWG',   sym: 'LSE:NWG',   kb: 'NWG.L'   },
    { label: 'STAN',  sym: 'LSE:STAN',  kb: 'STAN.L'  },
    { label: 'BP',    sym: 'LSE:BP',    kb: 'BP.L'    },
    { label: 'SHEL',  sym: 'LSE:SHEL',  kb: 'SHEL.L'  },
    { label: 'AZN',   sym: 'LSE:AZN',   kb: 'AZN.L'   },
    { label: 'GSK',   sym: 'LSE:GSK',   kb: 'GSK.L'   },
    { label: 'RIO',   sym: 'LSE:RIO',   kb: 'RIO.L'   },
    { label: 'AAL',   sym: 'LSE:AAL',   kb: 'AAL.L'   },
    { label: 'ULVR',  sym: 'LSE:ULVR',  kb: 'ULVR.L'  },
    { label: 'DGE',   sym: 'LSE:DGE',   kb: 'DGE.L'   },
    { label: 'TSCO',  sym: 'LSE:TSCO',  kb: 'TSCO.L'  },
    { label: 'NG',    sym: 'LSE:NG',    kb: 'NG.L'    },
    { label: 'REL',   sym: 'LSE:REL',   kb: 'REL.L'   },
    { label: 'CRH',   sym: 'LSE:CRH',   kb: 'CRH.L'   },
    { label: 'IMB',   sym: 'LSE:IMB',   kb: 'IMB.L'   },
    { label: 'PRU',   sym: 'LSE:PRU',   kb: 'PRU.L'   },
    { label: 'EXPN',  sym: 'LSE:EXPN',  kb: 'EXPN.L'  },
    { label: 'SGRO',  sym: 'LSE:SGRO',  kb: 'SGRO.L'  },
    { label: 'ANTO',  sym: 'LSE:ANTO',  kb: 'ANTO.L'  },
    { label: 'MNDI',  sym: 'LSE:MNDI',  kb: 'MNDI.L'  },
  ],
  us_equities: [
    { label: 'AAPL',  sym: 'NASDAQ:AAPL',  kb: 'AAPL'  },
    { label: 'MSFT',  sym: 'NASDAQ:MSFT',  kb: 'MSFT'  },
    { label: 'NVDA',  sym: 'NASDAQ:NVDA',  kb: 'NVDA'  },
    { label: 'GOOGL', sym: 'NASDAQ:GOOGL', kb: 'GOOGL' },
    { label: 'AMZN',  sym: 'NASDAQ:AMZN',  kb: 'AMZN'  },
    { label: 'META',  sym: 'NASDAQ:META',  kb: 'META'  },
    { label: 'TSLA',  sym: 'NASDAQ:TSLA',  kb: 'TSLA'  },
    { label: 'AVGO',  sym: 'NASDAQ:AVGO',  kb: 'AVGO'  },
    { label: 'AMD',   sym: 'NASDAQ:AMD',   kb: 'AMD'   },
    { label: 'INTC',  sym: 'NASDAQ:INTC',  kb: 'INTC'  },
    { label: 'JPM',   sym: 'NYSE:JPM',     kb: 'JPM'   },
    { label: 'BAC',   sym: 'NYSE:BAC',     kb: 'BAC'   },
    { label: 'GS',    sym: 'NYSE:GS',      kb: 'GS'    },
    { label: 'WFC',   sym: 'NYSE:WFC',     kb: 'WFC'   },
    { label: 'V',     sym: 'NYSE:V',       kb: 'V'     },
    { label: 'MA',    sym: 'NYSE:MA',      kb: 'MA'    },
    { label: 'BRK.B', sym: 'NYSE:BRK.B',  kb: 'BRK-B' },
    { label: 'XOM',   sym: 'NYSE:XOM',     kb: 'XOM'   },
    { label: 'CVX',   sym: 'NYSE:CVX',     kb: 'CVX'   },
    { label: 'JNJ',   sym: 'NYSE:JNJ',     kb: 'JNJ'   },
    { label: 'UNH',   sym: 'NYSE:UNH',     kb: 'UNH'   },
    { label: 'PFE',   sym: 'NYSE:PFE',     kb: 'PFE'   },
    { label: 'WMT',   sym: 'NYSE:WMT',     kb: 'WMT'   },
    { label: 'COIN',  sym: 'NASDAQ:COIN',  kb: 'COIN'  },
    { label: 'PLTR',  sym: 'NYSE:PLTR',    kb: 'PLTR'  },
  ],
  eu_equities: [
    { label: 'SAP',     sym: 'XETR:SAP',     kb: 'SAP'    },
    { label: 'SIE',     sym: 'XETR:SIE',     kb: 'SIE'    },
    { label: 'ALV',     sym: 'XETR:ALV',     kb: 'ALV'    },
    { label: 'DTE',     sym: 'XETR:DTE',     kb: 'DTE'    },
    { label: 'BAS',     sym: 'XETR:BAS',     kb: 'BAS'    },
    { label: 'BMW',     sym: 'XETR:BMW',     kb: 'BMW'    },
    { label: 'MBG',     sym: 'XETR:MBG',     kb: 'MBG'    },
    { label: 'VOW3',    sym: 'XETR:VOW3',    kb: 'VOW3'   },
    { label: 'ADS',     sym: 'XETR:ADS',     kb: 'ADS'    },
    { label: 'ASML',    sym: 'NASDAQ:ASML',  kb: 'ASML'   },
    { label: 'OR',      sym: 'EURONEXT:OR',  kb: 'OR'     },
    { label: 'MC',      sym: 'EURONEXT:MC',  kb: 'MC'     },
    { label: 'BNP',     sym: 'EURONEXT:BNP', kb: 'BNP'    },
    { label: 'SAN',     sym: 'BME:SAN',      kb: 'SAN'    },
    { label: 'BBVA',    sym: 'BME:BBVA',     kb: 'BBVA'   },
    { label: 'NOVO B',  sym: 'CPH:NOVO_B',   kb: 'NOVO-B' },
    { label: 'NESN',    sym: 'SWX:NESN',     kb: 'NESN'   },
    { label: 'ROG',     sym: 'SWX:ROG',      kb: 'ROG'    },
    { label: 'UHR',     sym: 'SWX:UHR',      kb: 'UHR'    },
  ],
  commodities: [
    { label: 'Gold',      sym: 'OANDA:XAUUSD',  kb: 'XAUUSD'  },
    { label: 'Silver',    sym: 'OANDA:XAGUSD',  kb: 'XAGUSD'  },
    { label: 'Platinum',  sym: 'OANDA:XPTUSD',  kb: 'XPTUSD'  },
    { label: 'Palladium', sym: 'OANDA:XPDUSD',  kb: 'XPDUSD'  },
    { label: 'Copper',    sym: 'OANDA:XCUUSD',  kb: 'XCUUSD'  },
    { label: 'Crude Oil', sym: 'TVC:USOIL',     kb: 'CL'      },
    { label: 'Brent',     sym: 'TVC:UKOIL',     kb: 'BZ'      },
    { label: 'Nat Gas',   sym: 'TVC:NATGAS',    kb: 'NG'      },
    { label: 'Wheat',     sym: 'CBOT:ZW1!',     kb: 'ZW'      },
    { label: 'Corn',      sym: 'CBOT:ZC1!',     kb: 'ZC'      },
    { label: 'Soybeans',  sym: 'CBOT:ZS1!',     kb: 'ZS'      },
    { label: 'Cotton',    sym: 'NYMEX:CT1!',    kb: 'CT'      },
    { label: 'Coffee',    sym: 'ICEUS:KC1!',    kb: 'KC'      },
    { label: 'Sugar',     sym: 'ICEUS:SB1!',    kb: 'SB'      },
    { label: 'Cocoa',     sym: 'ICEUS:CC1!',    kb: 'CC'      },
    { label: 'Lumber',    sym: 'CME:LB1!',      kb: 'LB'      },
  ],
  forex: [
    { label: 'EUR/USD', sym: 'FX:EURUSD',  kb: 'EURUSD'  },
    { label: 'GBP/USD', sym: 'FX:GBPUSD',  kb: 'GBPUSD'  },
    { label: 'USD/JPY', sym: 'FX:USDJPY',  kb: 'USDJPY'  },
    { label: 'GBP/EUR', sym: 'FX:GBPEUR',  kb: 'GBPEUR'  },
    { label: 'USD/CHF', sym: 'FX:USDCHF',  kb: 'USDCHF'  },
    { label: 'AUD/USD', sym: 'FX:AUDUSD',  kb: 'AUDUSD'  },
    { label: 'USD/CAD', sym: 'FX:USDCAD',  kb: 'USDCAD'  },
    { label: 'NZD/USD', sym: 'FX:NZDUSD',  kb: 'NZDUSD'  },
    { label: 'EUR/GBP', sym: 'FX:EURGBP',  kb: 'EURGBP'  },
    { label: 'EUR/JPY', sym: 'FX:EURJPY',  kb: 'EURJPY'  },
    { label: 'GBP/JPY', sym: 'FX:GBPJPY',  kb: 'GBPJPY'  },
    { label: 'USD/CNH', sym: 'FX:USDCNH',  kb: 'USDCNH'  },
    { label: 'USD/SGD', sym: 'FX:USDSGD',  kb: 'USDSGD'  },
    { label: 'DXY',     sym: 'TVC:DXY',    kb: 'DXY'     },
  ],
  crypto: [
    { label: 'BTC/USD',  sym: 'BITSTAMP:BTCUSD',   kb: 'BTC'   },
    { label: 'ETH/USD',  sym: 'BITSTAMP:ETHUSD',   kb: 'ETH'   },
    { label: 'BNB/USD',  sym: 'BINANCE:BNBUSD',    kb: 'BNB'   },
    { label: 'SOL/USD',  sym: 'BINANCE:SOLUSDT',   kb: 'SOL'   },
    { label: 'XRP/USD',  sym: 'BITSTAMP:XRPUSD',   kb: 'XRP'   },
    { label: 'ADA/USD',  sym: 'BINANCE:ADAUSDT',   kb: 'ADA'   },
    { label: 'AVAX/USD', sym: 'BINANCE:AVAXUSDT',  kb: 'AVAX'  },
    { label: 'DOGE/USD', sym: 'BINANCE:DOGEUSDT',  kb: 'DOGE'  },
    { label: 'DOT/USD',  sym: 'BINANCE:DOTUSDT',   kb: 'DOT'   },
    { label: 'LINK/USD', sym: 'BINANCE:LINKUSDT',  kb: 'LINK'  },
    { label: 'LTC/USD',  sym: 'BITSTAMP:LTCUSD',   kb: 'LTC'   },
    { label: 'MATIC',    sym: 'BINANCE:MATICUSDT',  kb: 'MATIC' },
    { label: 'UNI/USD',  sym: 'BINANCE:UNIUSDT',   kb: 'UNI'   },
    { label: 'ATOM/USD', sym: 'BINANCE:ATOMUSDT',  kb: 'ATOM'  },
  ],
  etfs: [
    { label: 'SPY',    sym: 'AMEX:SPY',     kb: 'SPY'   },
    { label: 'QQQ',    sym: 'NASDAQ:QQQ',   kb: 'QQQ'   },
    { label: 'IWM',    sym: 'AMEX:IWM',     kb: 'IWM'   },
    { label: 'DIA',    sym: 'AMEX:DIA',     kb: 'DIA'   },
    { label: 'GLD',    sym: 'AMEX:GLD',     kb: 'GLD'   },
    { label: 'SLV',    sym: 'AMEX:SLV',     kb: 'SLV'   },
    { label: 'USO',    sym: 'AMEX:USO',     kb: 'USO'   },
    { label: 'XLF',    sym: 'AMEX:XLF',     kb: 'XLF'   },
    { label: 'XLE',    sym: 'AMEX:XLE',     kb: 'XLE'   },
    { label: 'XLK',    sym: 'AMEX:XLK',     kb: 'XLK'   },
    { label: 'XLV',    sym: 'AMEX:XLV',     kb: 'XLV'   },
    { label: 'ARKK',   sym: 'AMEX:ARKK',    kb: 'ARKK'  },
    { label: 'IBTL',   sym: 'LSE:IBTL',     kb: 'IBTL'  },
    { label: 'VWRL',   sym: 'LSE:VWRL',     kb: 'VWRL'  },
    { label: 'ISF',    sym: 'LSE:ISF',      kb: 'ISF'   },
    { label: 'CSPX',   sym: 'LSE:CSPX',     kb: 'CSPX'  },
    { label: 'IGLT',   sym: 'LSE:IGLT',     kb: 'IGLT'  },
    { label: 'EMB',    sym: 'NASDAQ:EMB',   kb: 'EMB'   },
    { label: 'HYG',    sym: 'AMEX:HYG',     kb: 'HYG'   },
    { label: 'TLT',    sym: 'NASDAQ:TLT',   kb: 'TLT'   },
  ],
  options_etf: [
    { label: 'VIX',    sym: 'TVC:VIX',      kb: 'VIX'   },
    { label: 'UVXY',   sym: 'AMEX:UVXY',    kb: 'UVXY'  },
    { label: 'SVXY',   sym: 'AMEX:SVXY',    kb: 'SVXY'  },
    { label: 'VXX',    sym: 'AMEX:VXX',     kb: 'VXX'   },
    { label: 'VIXY',   sym: 'AMEX:VIXY',    kb: 'VIXY'  },
    { label: 'SQQQ',   sym: 'NASDAQ:SQQQ',  kb: 'SQQQ'  },
    { label: 'TQQQ',   sym: 'NASDAQ:TQQQ',  kb: 'TQQQ'  },
    { label: 'SPXS',   sym: 'AMEX:SPXS',    kb: 'SPXS'  },
    { label: 'SPXL',   sym: 'AMEX:SPXL',    kb: 'SPXL'  },
    { label: 'LABD',   sym: 'AMEX:LABD',    kb: 'LABD'  },
    { label: 'LABU',   sym: 'AMEX:LABU',    kb: 'LABU'  },
    { label: 'BOIL',   sym: 'AMEX:BOIL',    kb: 'BOIL'  },
    { label: 'KOLD',   sym: 'AMEX:KOLD',    kb: 'KOLD'  },
    { label: 'DUST',   sym: 'AMEX:DUST',    kb: 'DUST'  },
    { label: 'NUGT',   sym: 'AMEX:NUGT',    kb: 'NUGT'  },
  ],
};

const _TICKER_MAP = {
  'BARC.L': 'LSE:BARC', 'HSBA.L': 'LSE:HSBA', 'LLOY.L': 'LSE:LLOY',
  'LSEG.L': 'LSE:LSEG', 'NWG.L': 'LSE:NWG', 'STAN.L': 'LSE:STAN',
  'BP.L': 'LSE:BP', 'SHEL.L': 'LSE:SHEL', 'AZN.L': 'LSE:AZN',
  'XAUUSD': 'OANDA:XAUUSD', 'GOLD': 'OANDA:XAUUSD', 'GLD': 'AMEX:GLD',
  'BTCUSD': 'BITSTAMP:BTCUSD', 'BTC': 'BITSTAMP:BTCUSD',
  'ETHUSD': 'BITSTAMP:ETHUSD', 'ETH': 'BITSTAMP:ETHUSD',
  'SPY': 'AMEX:SPY', 'QQQ': 'NASDAQ:QQQ', 'IWM': 'AMEX:IWM',
  'AAPL': 'NASDAQ:AAPL', 'MSFT': 'NASDAQ:MSFT', 'NVDA': 'NASDAQ:NVDA',
  'TSLA': 'NASDAQ:TSLA', 'GOOGL': 'NASDAQ:GOOGL', 'AMZN': 'NASDAQ:AMZN',
  'META': 'NASDAQ:META', 'VIX': 'TVC:VIX', 'DXY': 'TVC:DXY',
  'CRUDE': 'TVC:USOIL', 'OIL': 'TVC:USOIL', 'BRENT': 'TVC:UKOIL',
  'EURUSD': 'FX:EURUSD', 'GBPUSD': 'FX:GBPUSD', 'USDJPY': 'FX:USDJPY',
};

function _resolveSymbol(raw) {
  const up = raw.trim().toUpperCase().replace(/\s+/g, '');
  if (_TICKER_MAP[up]) return _TICKER_MAP[up];
  if (up.includes(':')) return up;
  if (up.endsWith('.L')) return 'LSE:' + up.slice(0, -2);
  if (/^[A-Z]{6}$/.test(up) && !up.endsWith('USD')) return 'FX:' + up;
  return up;
}

function _renderChips(cat) {
  const chips = document.getElementById('markets-chips');
  chips.innerHTML = '';
  (_MARKET_CATS[cat] || []).forEach(item => {
    const el = document.createElement('span');
    el.className = 'markets-chip' + (item.sym === _tvCurrentSym ? ' active' : '');
    el.dataset.sym = item.sym;
    el.dataset.kb  = item.kb || item.label;
    el.textContent = item.label;
    el.addEventListener('click', () => {
      _loadMarketSymbol(item.sym, item.kb || item.label);
      document.getElementById('markets-ticker-input').value = '';
    });
    chips.appendChild(el);
  });
}

function _loadMarketSymbol(sym, kbTicker) {
  _tvCurrentSym = sym;
  _tvCurrentKb  = kbTicker;
  document.querySelectorAll('.markets-chip').forEach(c => {
    c.classList.toggle('active', c.dataset.sym === sym);
  });
  const label = document.getElementById('markets-current-label');
  if (label) label.textContent = kbTicker;
  _renderTVChart(sym);
  _loadKBPanel(kbTicker);
}

let _tvResizeObserver = null;

function _renderTVChart(sym) {
  const container = document.getElementById('tv-chart');
  container.innerHTML = '';
  window._tvWidgetInstance = null;

  // Tear down any previous ResizeObserver
  if (_tvResizeObserver) { _tvResizeObserver.disconnect(); _tvResizeObserver = null; }

  // Always use the srcdoc iframe approach — TradingView.widget constructor
  // requires a DOM-measured pixel height which is unreliable in flex layouts.
  // The embed-widget-advanced-chart srcdoc pattern is battle-tested here.
  function _buildIframe(tvSym) {
    const h = Math.max(container.clientHeight, 400);
    container.innerHTML = '';
    const iframe = document.createElement('iframe');
    iframe.src = `${API}/markets/chart?sym=${encodeURIComponent(tvSym)}`;
    iframe.style.cssText = `width:100%;height:${h}px;border:none;display:block;`;
    iframe.setAttribute('frameborder', '0');
    iframe.setAttribute('allowfullscreen', '');
    container.appendChild(iframe);
  }

  // Build immediately — if container has no height yet (flex not painted),
  // wait one rAF for the browser to measure layout first.
  if (container.clientHeight > 0) {
    _buildIframe(sym);
  } else {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => _buildIframe(sym));
    });
  }

  // Keep iframe height in sync if the window/panel resizes
  if (window.ResizeObserver) {
    _tvResizeObserver = new ResizeObserver(() => {
      const iframe = container.querySelector('iframe');
      if (iframe && container.clientHeight > 0) {
        iframe.style.height = container.clientHeight + 'px';
      }
    });
    _tvResizeObserver.observe(container);
  }
}

async function _loadKBPanel(kbTicker) {
  const panel = document.getElementById('kb-panel');
  panel.innerHTML = `<div class="kb-title">KB Intelligence — ${kbTicker}</div><div style="color:var(--muted);font-size:11px;">Loading…</div>`;
  try {
    const res = await fetch(`${API}/tickers/${encodeURIComponent(kbTicker)}/summary`);
    if (!res.ok) throw new Error('no data');
    const data = await res.json();
    _renderKBPanel(panel, data, kbTicker);
  } catch {
    panel.innerHTML = `<div class="kb-title">KB Intelligence — ${kbTicker}</div>
      <div style="color:var(--muted);font-size:11px;margin-top:6px;">No KB data for ${kbTicker}.</div>
      <button class="kb-ask-btn" onclick="askKBAbout('${kbTicker}')">Ask KB about ${kbTicker}</button>`;
  }
}

function _renderKBPanel(panel, data, ticker) {
  const tierClass = (t) => {
    if (!t) return 'kb-tier-low';
    const v = t.toLowerCase();
    if (v.includes('high')) return 'kb-tier-high';
    if (v.includes('med'))  return 'kb-tier-medium';
    return 'kb-tier-low';
  };
  const dirClass = (d) => {
    if (!d) return '';
    return d.toLowerCase().includes('bull') ? 'kb-dir-bullish' : d.toLowerCase().includes('bear') ? 'kb-dir-bearish' : '';
  };
  const fmt = (v, fallback) => (v !== null && v !== undefined) ? v : `<span style="color:var(--muted)">${fallback}</span>`;
  const stress = data.kb_stress;
  const stressColor = stress > 0.7 ? 'var(--red)' : stress > 0.4 ? 'var(--accent)' : 'var(--green)';

  let patternsHtml = '';
  if (data.open_patterns && data.open_patterns.length) {
    patternsHtml = `<div class="kb-title" style="margin-top:4px;">Open Patterns</div><div class="kb-patterns">`;
    data.open_patterns.forEach(p => {
      patternsHtml += `<div class="kb-pattern-item">${p.pattern_type} · ${p.direction || '—'} · ${p.timeframe || '—'} <span style="color:var(--muted)">[${(p.quality_score||0).toFixed(2)}]</span></div>`;
    });
    patternsHtml += '</div>';
  }

  let alertsHtml = '';
  if (data.recent_alerts && data.recent_alerts.length) {
    alertsHtml = `<div class="kb-title" style="margin-top:4px;">Recent Alerts</div><div class="kb-patterns">`;
    data.recent_alerts.slice(0,3).forEach(a => {
      alertsHtml += `<div class="kb-pattern-item">${a.alert_type || a.type || '—'}: ${(a.message||'').slice(0,60)}</div>`;
    });
    alertsHtml += '</div>';
  }

  const hasAnyData = data.conviction_tier || data.signal_quality || data.upside_pct || data.last_price;

  panel.innerHTML = `
    <div class="kb-title">KB Intelligence — ${ticker}</div>
    ${ hasAnyData ? `
    <div class="kb-row"><span class="label">Conviction</span><span class="${tierClass(data.conviction_tier)}">${fmt(data.conviction_tier, '—')}</span></div>
    <div class="kb-row"><span class="label">Direction</span><span class="${dirClass(data.signal_direction)}">${fmt(data.signal_direction, '—')}</span></div>
    <div class="kb-row"><span class="label">Last Price</span><span class="mono-amber">${fmt(data.last_price, '—')}</span></div>
    <div class="kb-row"><span class="label">Upside</span><span style="color:var(--green)">${data.upside_pct ? '+'+data.upside_pct+'%' : '<span style="color:var(--muted)">—</span>'}</span></div>
    <div class="kb-row"><span class="label">Invalidation</span><span class="mono">${fmt(data.invalidation_distance, '—')}</span></div>
    <div class="kb-row"><span class="label">Vol Regime</span><span class="mono">${fmt(data.volatility_regime, '—')}</span></div>
    <div class="kb-row"><span class="label">Signal Quality</span><span class="mono">${data.signal_quality ? parseFloat(data.signal_quality).toFixed(2) : '<span style="color:var(--muted)">—</span>'}</span></div>
    ${ stress !== null && stress !== undefined ? `
    <div>
      <div class="kb-row"><span class="label">KB Stress</span><span style="color:${stressColor}">${(stress*100).toFixed(0)}%</span></div>
      <div class="kb-stress-bar"><div class="kb-stress-fill" style="width:${(stress*100).toFixed(0)}%;background:${stressColor};"></div></div>
    </div>` : '' }
    ${patternsHtml}
    ${alertsHtml}
    ` : `<div style="color:var(--muted);font-size:11px;margin-top:4px;">No KB atoms for ${ticker}.</div>` }
    <button class="kb-ask-btn" onclick="askKBAbout('${ticker}')">Ask KB about ${ticker}</button>
  `;
}

function askKBAbout(ticker) {
  showScreen('chat');
  const inp = document.getElementById('chat-input');
  if (inp) {
    inp.value = `Tell me everything you know about ${ticker}`;
    inp.dispatchEvent(new Event('input'));
    document.getElementById('chat-send-btn')?.click();
  }
}

// ── Sector normalisation (shared with visualiser) ────────────────────────────
const _SECTOR_NORM = {
  'financial services': 'Financial Services', 'financial_services': 'Financial Services',
  'financials': 'Financial Services', 'financial': 'Financial Services',
  'technology': 'Technology', 'information technology': 'Technology', 'tech': 'Technology',
  'healthcare': 'Healthcare', 'health care': 'Healthcare',
  'consumer cyclical': 'Consumer', 'consumer discretionary': 'Consumer',
  'consumer defensive': 'Consumer', 'consumer staples': 'Consumer', 'consumer': 'Consumer',
  'energy': 'Energy',
  'industrials': 'Industrials', 'industrial': 'Industrials',
  'communication services': 'Communication', 'communications': 'Communication', 'communication': 'Communication',
  'real estate': 'Real Estate', 'reits': 'Real Estate',
  'utilities': 'Utilities',
  'basic materials': 'Materials', 'materials': 'Materials',
};
function _normSector(raw) {
  if (!raw) return 'Other';
  return _SECTOR_NORM[(raw || '').toLowerCase()] || raw;
}

async function _loadSectorPulse() {
  const el = document.getElementById('mkt-sector-pulse');
  if (!el) return;
  try {
    const snap = await apiFetch('/market/snapshot');
    const tickers = snap?.tickers || [];
    if (!tickers.length) return;
    const byS = {};
    tickers.forEach(t => {
      const s = _normSector(t.sector);
      if (!byS[s]) byS[s] = { count: 0, bull: 0, upsides: [] };
      byS[s].count++;
      if ((t.signal_direction || '').toLowerCase().includes('bull')) byS[s].bull++;
      const u = parseFloat(t.upside_pct);
      if (!isNaN(u)) byS[s].upsides.push(u);
    });
    const sectors = Object.entries(byS)
      .map(([name, d]) => ({
        name,
        count: d.count,
        bullPct: Math.round(d.bull / d.count * 100),
        avgUpside: d.upsides.length ? (d.upsides.reduce((a, b) => a + b, 0) / d.upsides.length).toFixed(1) : null,
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

    el.innerHTML = sectors.map(s => {
      const clr = s.bullPct >= 60 ? '#22c55e' : s.bullPct >= 40 ? '#f59e0b' : '#6b7280';
      const up  = s.avgUpside != null ? `Avg upside: ${s.avgUpside}%` : '';
      return `<div class="mkt-sector-tile" style="border-top-color:${clr}" onclick="_mktSectorClick('${escHtml(s.name)}')">
        <div class="mst-name">${escHtml(s.name)}</div>
        <div class="mst-count">${s.count} tickers</div>
        <div class="mst-bull" style="color:${clr}">↑ ${s.bullPct}% bullish</div>
        ${up ? `<div class="mst-upside">${escHtml(up)}</div>` : ''}
      </div>`;
    }).join('');
    el.style.display = '';
  } catch { /* silent */ }
}

window._mktSectorClick = function(sector) {
  window._visPrefilterSector = sector;
  navigate('visualiser');
};

function initMarketsScreen() {
  if (!_marketsInited) {
    document.querySelectorAll('.mcat-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.mcat-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        _marketsCat = tab.dataset.cat;
        _renderChips(_marketsCat);
        const first = _MARKET_CATS[_marketsCat]?.[0];
        if (first) _loadMarketSymbol(first.sym, first.kb || first.label);
      });
    });
    document.getElementById('markets-go-btn').addEventListener('click', () => {
      const raw = document.getElementById('markets-ticker-input').value.trim();
      if (!raw) return;
      _loadMarketSymbol(_resolveSymbol(raw), raw.toUpperCase());
    });
    document.getElementById('markets-ticker-input').addEventListener('keydown', e => {
      if (e.key === 'Enter') document.getElementById('markets-go-btn').click();
    });
    _marketsInited = true;
  }
  _renderChips(_marketsCat);
  _loadMarketSymbol(_tvCurrentSym, _tvCurrentKb);
  _loadSectorPulse();
}

