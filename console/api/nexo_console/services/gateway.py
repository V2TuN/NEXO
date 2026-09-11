"""Instance endpoint gateway.

On platforms that expose a single public HTTP endpoint per deployment
(e.g. Lucity's workload domain with Gateway API HTTPRoutes), every NEXO
instance is reached through the Console's public URL under a private
endpoint token:

    https://<console-host>/i/<endpoint-token>/<core-path>

The gateway authenticates by endpoint token (un guessable, rotatable),
strips the prefix, and proxies HTTP and WebSocket traffic to the instance's
NEXO Core through the Worker. On self-hosted deployments with wildcard DNS
the same instances can additionally be exposed as real hostnames via the
bundled Caddy — both modes share this proxy path.

WebSocket proxying is implemented frame-by-frame (client ⇄ gateway ⇄ core)
because the standard HTTP client stack cannot pass an Upgrade through.
"""
from __future__ import annotations

import asyncio
import httpx
import websockets
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response, StreamingResponse

from ..db import get_pool
from ..logging import get

log = get("network", "nexo.console.gateway")

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
    "authorization",  # replaced with the worker token below
}


FRIENDLY_404 = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>NEXO</title>
<style>body{{background:#0a0c10;color:#e7ebf3;font-family:-apple-system,Segoe UI,Roboto,sans-serif;
display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}}
.c{{max-width:420px;text-align:center;padding:28px;border:1px solid #1e2430;border-radius:12px;background:#12151c}}
h2{{margin:0 0 8px}}p{{color:#9aa4b8;font-size:13.5px;line-height:1.55}}
code{{background:#10131a;border:1px solid #2a3242;border-radius:6px;padding:1px 6px;font-size:12px}}</style></head>
<body><div class="c"><h2>{title}</h2><p>{body}</p></div></body></html>"""


def _page(title: str, body: str, status: int = 200) -> "HTMLResponse":
    from fastapi.responses import HTMLResponse

    return HTMLResponse(FRIENDLY_404.format(title=title, body=body), status_code=status)


import json as _json_mod  # noqa: E402

def _sub_html_page(title: str, configs: list, host: str, sub_path: str,
                   qr_path: str = "") -> str:
    '''Responsive NEXO subscription page.''' 
    import html as _html
    import io

    import qrcode
    import qrcode.image.svg

    esc = _html.escape

    def qr_svg(text: str, size: int = 9) -> str:
        img = qrcode.make(text, image_factory=qrcode.image.svg.SvgPathImage,
                          box_size=size, border=1)
        buf = io.BytesIO()
        img.save(buf)
        return buf.getvalue().decode()

    PROTO_META = {
        "vless-ws": ("VLESS", "WebSocket", "✦"),
        "trojan-ws": ("Trojan", "WebSocket", "◈"),
        "shadowsocks": ("Shadowsocks", "AEAD", "◉"),
        "xhttp-packet-up": ("xHTTP", "packet-up", "↗"),
        "xhttp-stream-up": ("xHTTP", "stream-up", "↗"),
    }
    cards = ""
    for i, c in enumerate(configs):
        pname, transport, icon = PROTO_META.get(c["protocol"], (c["protocol"], "", "•"))
        qr = qr_svg(c["share_url"], 8)
        cards += f'''
        <article class="config-card">
          <div class="config-head">
            <div class="config-title-wrap"><span class="config-icon">{icon}</span><div><div class="config-name">{esc(pname)}</div><div class="config-transport">{esc(transport)}</div></div></div>
            <span class="protocol-chip">{esc(c["protocol"])}</span>
          </div>
          <div class="url-box" id="u{i}">{esc(c["share_url"])}</div>
          <div class="config-actions"><button class="btn btn-primary btn-copy" data-copy="u{i}">کپی لینک</button><button class="btn btn-secondary qr-toggle" data-qr="q{i}">QR Code</button></div>
          <div class="qr-panel" id="q{i}" aria-hidden="true">{qr}<span>اسکن با کلاینت</span></div>
        </article>'''

    all_configs = "\n".join(c["share_url"] for c in configs if c.get("share_url"))
    all_configs_qr = qr_svg(all_configs, 7) if all_configs else ""
    sub_url = f"https://{host}{sub_path}"
    sub_qr = qr_svg(sub_url, 10)
    sb_url = f"https://{host}{sub_path}?fmt=singbox&host={host}"
    cl_url = f"https://{host}{sub_path}?fmt=clash&host={host}"

    html = '''<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#06111f"><meta name="color-scheme" content="dark">
<title>__TITLE__ · NEXO</title>
<style>
:root {--bg:#030a14;--surface:#071525;--surface2:#0a1b2e;--border:rgba(65,146,220,.22);--strong:rgba(38,157,255,.55);--text:#f2f7ff;--muted:#8da4bd;--blue:#149cff;--green:#27d98b;--red:#ff5870;--shadow:0 18px 50px rgba(0,0,0,.34);--radius:18px;--sans:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",Tahoma,Arial,sans-serif;--mono:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;min-height:100vh;color:var(--text);font-family:var(--sans);background:radial-gradient(900px 460px at 72% -12%,rgba(20,156,255,.20),transparent 64%),radial-gradient(680px 420px at 4% 60%,rgba(0,105,255,.10),transparent 68%),linear-gradient(180deg,#020812,#04101d 52%,#030a14)}
body:before{content:"";position:fixed;inset:0;pointer-events:none;opacity:.22;background-image:linear-gradient(rgba(255,255,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px);background-size:38px 38px}
a{color:inherit}button{font:inherit}.container{width:min(1120px,calc(100% - 32px));margin:0 auto;position:relative;z-index:1}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:20px 0 14px}.brand{display:flex;align-items:center;gap:12px}.brand-mark{width:44px;height:44px;display:grid;place-items:center;border-radius:14px;background:linear-gradient(145deg,#0c68c9,#06182c);border:1px solid var(--strong);box-shadow:0 0 28px rgba(20,156,255,.20);font-size:25px;font-weight:900;color:#6bd0ff}.brand-name{font-size:25px;font-weight:850;letter-spacing:.5px}.brand-sub{color:var(--muted);font-size:11px;margin-top:2px}.lang{border:1px solid var(--border);background:rgba(7,21,37,.78);color:var(--muted);border-radius:999px;padding:8px 14px;cursor:pointer}
.hero{overflow:hidden;position:relative;border:1px solid var(--border);border-radius:24px;padding:30px 32px;background:linear-gradient(110deg,rgba(7,25,44,.97),rgba(4,16,30,.90));box-shadow:var(--shadow);margin-bottom:18px}.hero:after{content:"";position:absolute;width:340px;height:340px;left:-80px;top:-170px;border-radius:50%;border:1px solid rgba(20,156,255,.22);box-shadow:0 0 90px rgba(20,156,255,.12),inset 0 0 70px rgba(20,156,255,.08)}.hero-grid{display:grid;grid-template-columns:1fr auto;gap:28px;align-items:center;position:relative;z-index:1}.eyebrow{display:inline-flex;align-items:center;gap:7px;color:#6ccfff;font-size:12px;font-weight:800;margin-bottom:10px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 13px rgba(39,217,139,.8)}.hero h1{margin:0;font-size:clamp(24px,4vw,38px);line-height:1.2}.hero p{margin:10px 0 0;color:var(--muted);line-height:1.8;max-width:650px;font-size:14px}.hero-visual{width:190px;height:130px;display:grid;place-items:center}.orbit{width:118px;height:118px;border-radius:50%;border:1px solid rgba(75,186,255,.45);box-shadow:0 0 42px rgba(20,156,255,.15),inset 0 0 34px rgba(20,156,255,.10);position:relative}.orbit:before,.orbit:after{content:"";position:absolute;inset:13px;border-radius:50%;border:1px dashed rgba(75,186,255,.28);transform:rotate(60deg) scaleX(1.5)}.orbit:after{transform:rotate(-60deg) scaleX(1.5)}.orbit span{position:absolute;inset:0;display:grid;place-items:center;font-weight:900;font-size:25px;text-shadow:0 0 22px #149cff}
.section{margin:18px 0}.card{background:linear-gradient(145deg,rgba(8,27,46,.96),rgba(4,16,29,.94));border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow);padding:22px}.card-head{display:flex;justify-content:space-between;align-items:flex-start;gap:15px;margin-bottom:17px}.card-title{margin:0;font-size:18px}.card-sub{color:var(--muted);font-size:12.5px;margin-top:5px;line-height:1.7}.status{color:#77efb5;background:rgba(39,217,139,.09);border:1px solid rgba(39,217,139,.25);padding:7px 12px;border-radius:999px;font-size:12px;white-space:nowrap}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}.stat{background:rgba(3,13,24,.72);border:1px solid var(--border);border-radius:15px;padding:15px}.stat-label{color:var(--muted);font-size:11px}.stat-value{margin-top:6px;font-size:18px;font-weight:800}.progress{height:7px;background:#0c2841;border-radius:999px;overflow:hidden;margin-top:10px}.progress i{display:block;width:32%;height:100%;background:linear-gradient(90deg,#087eff,#54c8ff);border-radius:inherit;box-shadow:0 0 18px rgba(20,156,255,.55)}
.sub-layout{display:grid;grid-template-columns:1fr 185px;gap:16px;align-items:stretch}.all-copy{margin-top:12px;padding:14px;border:1px solid rgba(20,156,255,.28);border-radius:15px;background:linear-gradient(135deg,rgba(10,42,70,.78),rgba(4,20,35,.72));display:flex;align-items:center;justify-content:space-between;gap:14px}.all-copy .copy-info{min-width:0}.all-copy strong{display:block;font-size:13px}.all-copy span{display:block;color:var(--muted);font-size:11px;margin-top:4px;line-height:1.6}.channel-corner{position:fixed;right:18px;bottom:18px;z-index:20;display:flex;align-items:center;gap:9px;padding:9px 12px;border:1px solid rgba(20,156,255,.4);border-radius:14px;background:rgba(4,18,32,.94);box-shadow:0 14px 35px rgba(0,0,0,.38);backdrop-filter:blur(12px);text-decoration:none}.channel-corner .tg{width:32px;height:32px;border-radius:50%;display:grid;place-items:center;background:#1599ff;color:white;font-weight:900}.channel-corner b{font-size:11px}.channel-corner small{display:block;color:#7f9ab5;font-size:9px;margin-top:2px}.url-wrap{display:flex;gap:10px;align-items:stretch}.url-box{flex:1;min-width:0;background:#020a13;border:1px solid rgba(73,147,209,.25);border-radius:13px;color:#a8c0d9;padding:13px 14px;font-family:var(--mono);font-size:11px;line-height:1.7;overflow-wrap:anywhere}.actions{display:flex;gap:9px;flex-wrap:wrap;margin-top:11px}.btn{border-radius:11px;border:1px solid var(--border);padding:10px 15px;cursor:pointer;font-weight:750;font-size:12px;transition:.15s ease}.btn:hover{transform:translateY(-1px)}.btn-primary{background:linear-gradient(135deg,#0788ff,#1267e7);color:white;border-color:#159dff;box-shadow:0 8px 24px rgba(0,111,255,.22)}.btn-secondary{background:#0b2035;color:#dcecff}.btn-danger{background:rgba(255,88,112,.08);color:#ff8fa0;border-color:rgba(255,88,112,.3)}.qr-main{min-height:185px;border:1px solid var(--border);border-radius:15px;background:#04101d;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;padding:12px}.qr-main svg{width:135px;height:135px;background:#fff;border-radius:10px;padding:5px}.qr-main span{color:var(--muted);font-size:10.5px}.format-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.format{text-decoration:none;padding:8px 11px;border:1px solid var(--border);background:#081a2d;border-radius:10px;color:#9ccfff;font-size:11px}
.quick-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.quick{text-decoration:none;min-height:105px;padding:15px 10px;border:1px solid var(--border);background:linear-gradient(180deg,#09203a,#061426);border-radius:15px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;transition:.15s ease;text-align:center}.quick:hover{transform:translateY(-2px);border-color:var(--strong);box-shadow:0 10px 30px rgba(20,156,255,.10)}.quick-icon{font-size:24px;color:#55c8ff}.quick b{font-size:12px}.quick span:last-child{color:var(--muted);font-size:10px}
.config-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}.config-card{background:rgba(2,11,21,.70);border:1px solid var(--border);border-radius:15px;padding:15px;min-width:0}.config-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.config-title-wrap{display:flex;align-items:center;gap:10px;min-width:0}.config-icon{width:36px;height:36px;display:grid;place-items:center;border-radius:11px;background:#082849;color:#55c8ff;font-size:18px;border:1px solid rgba(20,156,255,.3)}.config-name{font-size:14px;font-weight:800}.config-transport{color:var(--muted);font-size:10.5px;margin-top:2px}.protocol-chip{color:#82b9e8;border:1px solid var(--border);border-radius:999px;padding:4px 8px;font:10px var(--mono);white-space:nowrap}.config-actions{display:flex;gap:8px;margin-top:9px}.config-actions .btn{flex:1}.qr-panel{display:none;margin-top:12px;padding:14px;border-radius:12px;background:#fff;text-align:center}.qr-panel.show{display:flex;flex-direction:column;align-items:center;gap:8px}.qr-panel svg{width:190px;height:190px;max-width:100%}.qr-panel span{color:#26384a;font-size:10px}
.apps{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.app{text-decoration:none;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:13px 14px;border:1px solid var(--border);border-radius:13px;background:#07192c}.app b{font-size:12px}.app span{color:var(--muted);font-size:10px}.security{display:grid;grid-template-columns:1fr auto;gap:18px;align-items:center;background:linear-gradient(110deg,rgba(4,41,38,.72),rgba(5,25,34,.84));border-color:rgba(39,217,139,.22)}.security-copy{display:flex;align-items:center;gap:13px}.security-icon{width:48px;height:48px;display:grid;place-items:center;border-radius:15px;background:rgba(39,217,139,.10);border:1px solid rgba(39,217,139,.25);color:#63efad;font-size:22px}.security h3{margin:0 0 4px;font-size:14px}.security p{margin:0;color:#8ab6a5;font-size:11.5px;line-height:1.7}.footer{text-align:center;color:#5f7892;font-size:11px;padding:22px 0 36px}.footer a{color:#5dbfff;text-decoration:none}.sep{margin:0 7px;color:#31506b}.mobile-bar{display:none}.toast{position:fixed;z-index:100;bottom:25px;left:50%;transform:translateX(-50%) translateY(12px);opacity:0;pointer-events:none;background:#0b2239;border:1px solid var(--strong);color:#dff3ff;padding:10px 17px;border-radius:999px;box-shadow:var(--shadow);transition:.18s ease;font-size:12px}.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.en-text{display:none}body.en .fa-text{display:none}body.en .en-text{display:inline}body.en{direction:ltr}body.en .hero{text-align:left}body.en .card-head{text-align:left}
@media(max-width:820px){.container{width:min(100% - 22px,680px)}.brand-mark{width:39px;height:39px;border-radius:12px}.brand-name{font-size:22px}.hero{padding:23px 19px;border-radius:20px}.hero-grid{grid-template-columns:1fr}.hero-visual{display:none}.stats{gap:8px}.stat{padding:12px 9px}.stat-value{font-size:15px}.sub-layout{grid-template-columns:1fr}.qr-main{min-height:0;flex-direction:row;justify-content:flex-start}.qr-main svg{width:100px;height:100px}.quick-grid{grid-template-columns:repeat(5,1fr);gap:7px}.quick{min-height:90px;padding:10px 5px}.quick-icon{font-size:20px}.config-grid{grid-template-columns:1fr}.apps{grid-template-columns:1fr 1fr}}
@media(max-width:520px){.container{width:calc(100% - 14px)}.topbar{padding:10px 2px}.brand-sub{display:none}.lang{padding:7px 11px;font-size:11px}.hero h1{font-size:23px}.hero p{font-size:12px}.card{padding:15px;border-radius:15px}.card-title{font-size:16px}.stats{grid-template-columns:1fr 1fr 1fr}.stat-label{font-size:9.5px}.stat-value{font-size:13px}.url-wrap{display:block}.url-box{min-height:68px}.url-wrap .btn{width:100%;margin-top:8px}.actions{display:grid;grid-template-columns:1fr 1fr}.actions .btn:first-child{grid-column:1 / -1}.format-row{display:grid;grid-template-columns:1fr}.quick-grid{overflow-x:auto;grid-template-columns:repeat(5,88px);padding-bottom:3px;scrollbar-width:none}.quick-grid::-webkit-scrollbar{display:none}.quick{min-height:82px}.config-actions{display:grid;grid-template-columns:1fr 1fr}.apps{grid-template-columns:1fr}.security{grid-template-columns:1fr}.security-copy{align-items:flex-start}.security .btn{width:100%}.footer{padding-bottom:78px}.mobile-bar{display:flex;position:fixed;bottom:0;left:0;right:0;z-index:90;padding:7px 8px calc(7px + env(safe-area-inset-bottom));background:rgba(3,12,23,.94);border-top:1px solid var(--border);backdrop-filter:blur(16px)}.mobile-bar a{flex:1;text-decoration:none;text-align:center;color:#7691aa;font-size:9px;padding:5px 0}.mobile-bar a b{display:block;color:#b9dcf8;font-size:16px;line-height:1.2;margin-bottom:2px}}
</style>
</head>
<body>
<div class="container">
<header class="topbar"><div class="brand"><div class="brand-mark">N</div><div><div class="brand-name">NEXO</div><div class="brand-sub">Secure · Fast · Simple</div></div></div><button class="lang" id="langBtn" type="button">English</button></header>
<section class="hero"><div class="hero-grid"><div><div class="eyebrow"><i class="dot"></i><span class="fa-text">اشتراک NEXO</span><span class="en-text">NEXO SUBSCRIPTION</span></div><h1 class="fa-text">لینک اشتراک و کانفیگ‌های شما آماده است</h1><h1 class="en-text">Your subscription and configs are ready</h1><p class="fa-text">یک لینک برای همه کانفیگ‌ها؛ با کلاینت دلخواه وارد کنید و هر زمان لازم بود لینک یا QR را کپی کنید.</p><p class="en-text">One subscription for all configs. Import it into your client, or use the QR and individual config links below.</p></div><div class="hero-visual"><div class="orbit"><span>NEXO</span></div></div></div></section>
<section class="section card" id="subscription"><div class="card-head"><div><h2 class="card-title fa-text">اشتراک من</h2><h2 class="card-title en-text">My subscription</h2><div class="card-sub fa-text">وضعیت و مصرف فعلی اشتراک</div><div class="card-sub en-text">Current subscription status and usage</div></div><span class="status">● <span class="fa-text">فعال</span><span class="en-text">Active</span></span></div>
<div class="stats"><div class="stat"><div class="stat-label fa-text">تاریخ انقضا</div><div class="stat-label en-text">EXPIRES</div><div class="stat-value">1404/01/15</div><div class="stat-label fa-text" style="color:#58c4ff;margin-top:5px">۲۸ روز باقی‌مانده</div><div class="stat-label en-text" style="color:#58c4ff;margin-top:5px">28 days left</div></div><div class="stat"><div class="stat-label fa-text">مصرف شده</div><div class="stat-label en-text">USED</div><div class="stat-value">32 GB / 100 GB</div><div class="progress"><i></i></div></div><div class="stat"><div class="stat-label fa-text">دستگاه‌های متصل</div><div class="stat-label en-text">DEVICES</div><div class="stat-value">2 / 5</div><div class="stat-label fa-text" style="margin-top:5px">دستگاه فعال</div><div class="stat-label en-text" style="margin-top:5px">active devices</div></div></div>
<div class="sub-layout"><div><div class="card-sub fa-text" style="margin-bottom:8px;color:#d8eaff;font-weight:700">لینک اشتراک</div><div class="card-sub en-text" style="margin-bottom:8px;color:#d8eaff;font-weight:700">Subscription URL</div><div class="url-wrap"><div class="url-box" id="subUrl">__SUB_URL__</div><button class="btn btn-primary btn-copy" data-copy="subUrl">کپی</button></div><div class="actions"><button class="btn btn-primary" id="shareBtn">اشتراک‌گذاری</button><button class="btn btn-secondary" id="copyAgain">کپی لینک</button><a class="btn btn-secondary" href="__SB_URL__" target="_blank" rel="noopener">sing-box JSON</a><a class="btn btn-secondary" href="__CL_URL__" target="_blank" rel="noopener">Clash Meta</a></div><div class="format-row"><a class="format" href="__SUB_URL__" target="_blank" rel="noopener">v2ray Base64</a><a class="format" href="__SB_URL__" target="_blank" rel="noopener">Sing-box</a><a class="format" href="__CL_URL__" target="_blank" rel="noopener">Clash Meta YAML</a></div><div class="all-copy"><div class="copy-info"><strong>📋 کپی همه کانفیگ‌ها</strong><span>تمام کانفیگ‌های موجود را یکجا کپی کنید و داخل کلاینت موردنظر وارد کنید.</span></div><button class="btn btn-primary" id="copyAll">کپی همه</button></div></div><div class="qr-main">__SUB_QR__<span class="fa-text">اسکن لینک اشتراک با کلاینت</span><span class="en-text">Scan with your client</span></div></div></section>
<section class="section card" id="quick"><div class="card-head"><div><h2 class="card-title fa-text">اتصال سریع</h2><h2 class="card-title en-text">Quick connect</h2><div class="card-sub fa-text">کلاینت مناسب سیستم خود را انتخاب کنید</div><div class="card-sub en-text">Choose the client platform you use</div></div></div><div class="quick-grid"><a class="quick" href="https://github.com/MatsuriDayo/v2rayNG/releases" target="_blank" rel="noopener"><span class="quick-icon">⌁</span><b>Android</b><span>v2rayNG</span></a><a class="quick" href="https://github.com/MatsuriDayo/nekoray/releases" target="_blank" rel="noopener"><span class="quick-icon">◉</span><b>Android</b><span>NekoBox</span></a><a class="quick" href="https://apps.apple.com/app/streisand/id6490569503" target="_blank" rel="noopener"><span class="quick-icon"></span><b>iOS</b><span>Streisand</span></a><a class="quick" href="https://github.com/SagerNet/sing-box/releases" target="_blank" rel="noopener"><span class="quick-icon">▣</span><b>Windows</b><span>sing-box</span></a><a class="quick" href="https://github.com/SagerNet/sing-box/releases" target="_blank" rel="noopener"><span class="quick-icon">◌</span><b>macOS / Linux</b><span>sing-box</span></a></div></section>
<section class="section card" id="configs"><div class="card-head"><div><h2 class="card-title fa-text">کانفیگ‌های جداگانه</h2><h2 class="card-title en-text">Individual configs</h2><div class="card-sub fa-text">در صورت نیاز، هر کانفیگ را جداگانه کپی یا QR کنید</div><div class="card-sub en-text">Copy or scan any individual config when needed</div></div></div><div class="config-grid">__CARDS__</div></section>

<footer class="footer"><span>NEXO</span><span class="sep">·</span><span>Secure · Fast · Simple</span><span class="sep">·</span><a href="https://github.com/V2TuN/NEXO" target="_blank" rel="noopener">GitHub</a><span class="sep">·</span><a href="https://t.me/V2rayTun0" target="_blank" rel="noopener">@V2rayTun0</a><span class="sep">·</span><span>Created by <a href="https://t.me/Mehtif" target="_blank" rel="noopener">@Mehtif</a></span></footer>
</div>
<a class="channel-corner" href="https://t.me/V2rayTun0" target="_blank" rel="noopener" aria-label="عضویت در کانال @V2rayTun0"><span class="tg">➤</span><span><b>عضویت در کانال</b><small>@V2rayTun0</small></span></a><nav class="mobile-bar"><a href="#subscription"><b>⌂</b>اشتراک</a><a href="#quick"><b>↗</b>اتصال</a><a href="#configs"><b>◈</b>کانفیگ</a></nav><div class="toast" id="toast"></div>
<script>
(function() {
var toast=document.getElementById('toast'),timer;
function show(m){toast.textContent=m;toast.classList.add('show');clearTimeout(timer);timer=setTimeout(function(){toast.classList.remove('show')},1800)}
async function copy(id){var t=document.getElementById(id).textContent.trim();try{await navigator.clipboard.writeText(t);show(document.body.classList.contains('en')?'Copied ✓':'لینک کپی شد ✓')}catch(e){show('کپی انجام نشد')}}
document.querySelectorAll('.btn-copy').forEach(function(b){b.addEventListener('click',function(){copy(b.dataset.copy)})});document.getElementById('copyAgain').addEventListener('click',function(){copy('subUrl')});
var copyAll=document.getElementById('copyAll'); if(copyAll){copyAll.addEventListener('click',async function(){var t=__ALL_CONFIGS__;try{await navigator.clipboard.writeText(t);show('همه کانفیگ‌ها کپی شد ✓')}catch(e){show('کپی انجام نشد')}})}
document.querySelectorAll('.qr-toggle').forEach(function(b){b.addEventListener('click',function(){var e=document.getElementById(b.dataset.qr),open=e.classList.toggle('show');e.setAttribute('aria-hidden',open?'false':'true');b.textContent=open?'بستن QR':'QR Code'})});
document.getElementById('shareBtn').addEventListener('click',async function(){var url=document.getElementById('subUrl').textContent.trim();if(navigator.share){try{await navigator.share({title:'NEXO Subscription',url:url})}catch(e){}}else{copy('subUrl')}});
document.getElementById('langBtn').addEventListener('click',function(){var en=document.body.classList.toggle('en');this.textContent=en?'فارسی':'English';document.documentElement.lang=en?'en':'fa';document.documentElement.dir=en?'ltr':'rtl'});
})();
</script></body></html>'''
    return (html
        .replace('__TITLE__', esc(title))
        .replace('__SUB_URL__', esc(sub_url))
        .replace('__SB_URL__', esc(sb_url))
        .replace('__CL_URL__', esc(cl_url))
        .replace('__SUB_QR__', sub_qr)
        .replace('__ALL_CONFIGS__', _json_mod.dumps(all_configs, ensure_ascii=False))
        .replace('__CARDS__', cards))

def _singbox_outbound(url: str) -> dict:
    """vless:// / trojan:// URI -> sing-box outbound. Shadowsocks links pass
    through parsed minimally; unsupported schemes are skipped by caller."""
    import base64 as _b64u
    from urllib.parse import urlparse, parse_qs, unquote

    u = urlparse(url)
    q = {k: v[0] for k, v in parse_qs(u.query).items()}
    tag = unquote(u.fragment) or "nexo"
    common = {"tag": tag}
    if u.scheme in ("vless", "trojan"):
        inner = {
            "server": u.hostname or "",
            "server_port": u.port or 443,
            "uuid": u.username or "" if u.scheme == "vless" else None,
            "password": u.username or "" if u.scheme == "trojan" else None,
            "tls": {
                "enabled": q.get("security") == "tls",
                "server_name": q.get("sni") or u.hostname or "",
                "utls": {"enabled": True, "fingerprint": q.get("fp", "chrome")} if q.get("fp") else None,
            },
            "transport": {
                "type": "ws",
                "path": q.get("path", "/"),
                "headers": {"Host": q.get("host") or u.hostname or ""},
            } if q.get("type") == "ws" else None,
        }
        common["type"] = u.scheme
        out = {k: v for k, v in inner.items() if v is not None}
        tls = out.get("tls") or {}
        if tls.get("utls") is None:
            tls.pop("utls", None)
        if out.get("transport") is None:
            out.pop("transport", None)
        out.update(common)
        return out
    if u.scheme == "ss":
        userinfo = u.username or ""
        pad = "=" * (-len(userinfo) % 4)
        try:
            method, password = _b64u.b64decode(userinfo + pad).decode().split(":", 1)
        except Exception:
            method, password = "aes-256-gcm", ""
        return {"type": "shadowsocks", "tag": tag, "server": u.hostname or "",
                "server_port": u.port or 443, "method": method, "password": password}
    return {"type": u.scheme, "tag": tag}


def _clash_proxy(url: str) -> dict | None:
    """vless/trojan URI -> Clash Meta proxy map (vless needs Meta)."""
    from urllib.parse import urlparse, parse_qs, unquote

    u = urlparse(url)
    q = {k: v[0] for k, v in parse_qs(u.query).items()}
    name = unquote(u.fragment) or "nexo"
    if u.scheme == "vless":
        return {"name": name, "type": "vless", "server": u.hostname or "",
                "port": u.port or 443, "uuid": u.username or "",
                "udp": True, "tls": q.get("security") == "tls",
                "servername": q.get("sni") or u.hostname or "",
                "client-fingerprint": q.get("fp", "chrome"),
                "network": "ws", "ws-opts": {"path": q.get("path", "/"),
                "headers": {"Host": q.get("host") or u.hostname or ""}}}
    if u.scheme == "trojan":
        return {"name": name, "type": "trojan", "server": u.hostname or "",
                "port": u.port or 443, "password": u.username or "",
                "udp": True, "sni": q.get("sni") or u.hostname or "",
                "client-fingerprint": q.get("fp", "chrome"),
                "network": "ws", "ws-opts": {"path": q.get("path", "/"),
                "headers": {"Host": q.get("host") or u.hostname or ""}}}
    if u.scheme == "ss":
        import base64 as _b64u

        pad = "=" * (-len(u.username or "") % 4)
        try:
            method, password = _b64u.b64decode((u.username or "") + pad).decode().split(":", 1)
        except Exception:
            return None
        return {"name": name, "type": "ss", "server": u.hostname or "",
                "port": u.port or 443, "cipher": method, "password": password}
    return None


def _clash_quote(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'


def _clash_inline(p: dict) -> str:
    import json as _json

    return _json.dumps(p, ensure_ascii=False)


router = APIRouter(include_in_schema=False)


async def _resolve_endpoint(request: Request, token: str) -> dict | None:
    """endpoint token -> {instance_id, worker_url, status}"""
    pool = get_pool(request)
    from ..config import settings as _s
    from ..security.token_codec import decode_token

    log.info("resolve enter: token[:20]=%s", token[:20])
    instance_id = decode_token(token, _s.secret_key)
    log.info("resolve: token[:16]=%s decoded=%s", token[:16], instance_id)
    if instance_id is not None:
        row = await pool.fetchrow(
            """
            SELECT i.id, i.status,
                   (SELECT d.domain FROM domains d WHERE d.instance_id = i.id
                     AND d.is_active = TRUE AND d.kind = 'path'
                     ORDER BY d.created_at DESC LIMIT 1) AS endpoint_token,
                   (SELECT dep.node_id FROM deployments dep WHERE dep.instance_id = i.id
                     ORDER BY dep.started_at DESC LIMIT 1) AS node_id
            FROM instances i WHERE i.id = $1
            """,
            instance_id,
        )
    else:
        row = await pool.fetchrow(
            """
            SELECT i.id, i.status,
                   (SELECT d.domain FROM domains d WHERE d.instance_id = i.id
                     AND d.is_active = TRUE AND d.kind = 'path'
                     ORDER BY d.created_at DESC LIMIT 1) AS endpoint_token,
                   (SELECT dep.node_id FROM deployments dep WHERE dep.instance_id = i.id
                     ORDER BY dep.started_at DESC LIMIT 1) AS node_id
            FROM instances i
            WHERE i.id IN (SELECT instance_id FROM domains
                            WHERE kind = 'path' AND domain = $1 AND is_active = TRUE)
            """,
            token,
        )
    if row is not None and row["endpoint_token"] is None:
        row = None
    if row is None or row["status"] != "running":
        return None
    from ..services.workers import worker_url_for

    return {
        "instance_id": str(row["id"]),
        "worker_url": worker_url_for(row["node_id"] or "local"),
        "upstream": f"/worker/api/instances/{row['id']}/proxy",
    }


@router.get("/i/{token}")
async def instance_status_page(token: str, request: Request):
    """Browser-friendly view of a proxy endpoint (the path itself is for
    proxy clients, not people)."""
    target = await _resolve_endpoint(request, token)
    if target is None:
        return _page(
            "Endpoint not found",
            "This endpoint doesn't exist or its instance was removed. "
            "If you recently redeployed NEXO without a persistent volume, "
            "create a new instance in the panel and copy its fresh config "
            "from the <b>Config</b> tab.",
            status=404,
        )
    return _page(
        "This endpoint is live",
        "This address is the private transport path for your proxy client — "
        "there is no web page here. Open the NEXO panel, choose your "
        "instance, open the <b>Config</b> tab and copy the "
        "<code>vless://</code> link into your client (v2rayNG, NekoBox, "
        "Streisand, …).",
    )


@router.post("/i/{token}/api/qr")
async def instance_qr_public(token: str, request: Request):
    """QR (SVG) for the subscription page — authorized by the endpoint token."""
    import io

    import qrcode
    import qrcode.image.svg
    from fastapi.responses import Response as _Response

    target = await _resolve_endpoint(request, token)
    if target is None:
        raise HTTPException(status_code=404, detail="unknown endpoint")
    body = await request.json()
    text = str(body.get("text") or "")[:4096]
    if not text:
        raise HTTPException(status_code=400, detail="text required")
    img = qrcode.make(text, image_factory=qrcode.image.svg.SvgPathImage, box_size=12, border=2)
    buf = io.BytesIO()
    img.save(buf)
    return _Response(content=buf.getvalue(), media_type="image/svg+xml")


@router.get("/i/{token}/sub")
async def instance_subscription(token: str, request: Request):
    """Subscription: ALL protocols of this instance. Auth = endpoint token.
    Formats via ?fmt=: singbox | clash | (default) base64 v2ray list.
    Content host: ?host=, panel-announced host, or request host."""
    import base64 as _b64

    import httpx as _httpx

    from ..config import settings as _settings

    fmt = (request.query_params.get("fmt") or "").strip().lower()

    target = await _resolve_endpoint(request, token)
    if target is None:
        return _page(
            "Endpoint not found",
            "This subscription doesn't exist or its instance was removed. "
            "Create a new instance in the NEXO panel and copy its "
            "subscription URL from the Config tab.",
            status=404,
        )
    pool = get_pool(request)
    inst = await pool.fetchrow(
        "SELECT name, public_host, status FROM instances WHERE id = $1", target["instance_id"]
    )
    if inst is None or inst["status"] != "running":
        return _page("Instance not running",
                     "The subscription will work once the instance is running.", status=503)
    host = (request.query_params.get("host")
            or inst["public_host"]
            or (request.headers.get("x-forwarded-host") or "").split(",")[0].strip()
            or request.headers.get("host") or "").split(":")[0]
    if not host:
        return _page("Missing host", "Append ?host=<your-domain> to this URL.", status=400)
    try:
        async with _httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{target['worker_url'].rstrip('/')}{target['upstream']}/core/api/share",
                json={"host": host, "path_prefix": f"/i/{token}", "uuids": []},
                headers={"Authorization": f"Bearer {_settings.worker_token}",
                         "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            configs = [c for c in resp.json().get("links", []) if c.get("share_url")]
            links = [c["share_url"] for c in configs]
    except Exception as exc:
        return _page("Unavailable", f"Could not read the instance configs: {str(exc)[:160]}",
                     status=502)
    title = f"NEXO \u00b7 {inst['name']}"
    from fastapi.responses import Response as _Response

    # ── Browser detection: HTML page for people, raw payload for clients ──
    # Client apps (v2rayNG, NekoBox, sing-box, Clash, Streisand…) send UA
    # fragments that don't look like a browser. Explicit ?fmt= always wins.
    ua = (request.headers.get("user-agent") or "").lower()
    client_markers = ("v2ray", "neko", "sing-box", "singbox", "sfa", "sfi",
                      "clash", "mihomo", "stash", "flclash", "streisand",
                      "happ", "karing", "shadowrocket", "aras")
    looks_like_browser = "mozilla" in ua and not any(m in ua for m in client_markers)

    if looks_like_browser and not fmt:
        from fastapi.responses import HTMLResponse

        return HTMLResponse(_sub_html_page(title, configs, host, f"/i/{token}/sub",
                                           qr_path=f"/i/{token}/api/qr"))

    def _headers(extra: dict | None = None) -> dict:
        h = {
            "profile-title": "base64:" + _b64.b64encode(title.encode()).decode(),
            "subscription-userinfo": "upload=0; download=0; total=0; expire=0",
            "profile-update-interval": "24",
            "profile-web-page-url": f"{request.url.scheme}://{request.headers.get('host', host)}",
            "support-url": "https://t.me/V2rayTun0",
        }
        if extra:
            h.update(extra)
        return h

    # Format negotiation:
    #   ?fmt=singbox  -> sing-box JSON (Outbounds)
    #   ?fmt=clash    -> Clash YAML (proxies)
    #   default       -> base64 v2ray list (v2rayNG, NekoBox, Streisand, ArasClient)
    if fmt in ("singbox", "sing-box", "sb"):
        import json as _json

        outbounds = [_singbox_outbound(u) for u in links]
        payload = _json.dumps({"outbounds": outbounds}, ensure_ascii=False, indent=2)
        return _Response(content=payload, media_type="application/json",
                         headers=_headers({"subscription-userinfo": "upload=0; download=0; total=0; expire=0"}))

    if fmt in ("clash", "clash-meta", "yaml"):
        proxies = [_clash_proxy(u) for u in links]
        proxies = [p for p in proxies if p]
        names = [p["name"] for p in proxies]
        payload = (
            "port: 7890\nsocks-port: 7891\nallow-lan: false\nmode: rule\nlog-level: warning\n"
            "proxies:\n"
            + "\n".join("  - " + _clash_inline(p) for p in proxies)
            + "\nproxy-groups:\n  - name: NEXO\n    type: select\n    proxies:\n"
            + "".join(f"      - {_clash_quote(n)}\n" for n in names)
            + "rules:\n  - MATCH,NEXO\n"
        )
        return _Response(content=payload, media_type="text/yaml", headers=_headers())

    body = _b64.b64encode("\n".join(links).encode()).decode()
    return _Response(content=body, media_type="text/plain", headers=_headers())


@router.api_route("/i/{token}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def instance_http_gateway(token: str, path: str, request: Request):
    target = await _resolve_endpoint(request, token)
    if target is None:
        return _page(
            "Endpoint not found",
            "This endpoint doesn't exist or its instance is not running. "
            "Check the panel — if the instance is Running, copy the fresh "
            "config from its <b>Config</b> tab.",
            status=404,
        )

    worker_url = target["worker_url"].rstrip("/")
    url = f"{worker_url}{target['upstream']}/{path}"
    if request.url.query:
        url += f"?{request.url.query}"
    headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP]
    headers.append(("X-NEXO-Endpoint", token))
    from ..config import settings as _cfg

    headers.append(("Authorization", f"Bearer {_cfg.worker_token}"))

    client = httpx.AsyncClient(timeout=None)
    try:
        if request.method in ("GET", "HEAD", "OPTIONS"):
            upstream_req = client.build_request(
                request.method, url, headers=headers, params=None,
            )
            upstream_resp = await client.send(upstream_req, stream=True)
            return StreamingResponse(
                upstream_resp.aiter_raw(),
                status_code=upstream_resp.status_code,
                headers={k: v for k, v in upstream_resp.headers.items()
                         if k.lower() not in HOP_BY_HOP},
                background=_close_client(client, upstream_resp),
            )

        body = await request.body()
        upstream_resp = await client.request(request.method, url, headers=headers, content=body)
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers={k: v for k, v in upstream_resp.headers.items()
                     if k.lower() not in HOP_BY_HOP},
        )
    except httpx.HTTPError:
        raise HTTPException(status_code=502, detail="instance upstream unavailable")


def _close_client(client: httpx.AsyncClient, resp):
    from starlette.background import BackgroundTask

    async def _cleanup() -> None:
        await resp.aclose()
        await client.aclose()

    return BackgroundTask(_cleanup)


@router.websocket("/i/{token}/{path:path}")
async def instance_ws_gateway(ws: WebSocket, token: str, path: str):
    """Frame-level WebSocket relay into the instance's NEXO Core.

    Accept the client up front (so failures produce proper close codes, not
    Starlette's HTTP 403 rejection), then open the upstream through the
    worker's ws-proxy and pump frames in both directions.
    """
    await ws.accept()
    try:
        target = await _resolve_endpoint(ws, token)
    except Exception as _exc:
        import traceback as _tb

        log.error("WS resolve failed: %s | %s", _exc, _tb.format_exc()[-400:])
        target = None
    if target is None:
        await ws.close(code=1008, reason="unknown or inactive instance endpoint")
        return

    # Build the upstream ws URL through the worker's websocket proxy
    # (separate route from the HTTP /proxy path).
    worker_ws = target["worker_url"].replace("http://", "ws://").replace("https://", "wss://").rstrip("/")
    from ..config import settings as _settings

    upstream_url = (
        f"{worker_ws}/worker/api/instances/{target['instance_id']}/ws-proxy/{path}"
        f"?token={_settings.worker_token}"
    )

    # Forward selected client headers so Core sees the real client IP etc.
    client_headers = {}
    for key in ("x-forwarded-for", "x-real-ip", "user-agent"):
        val = ws.headers.get(key)
        if val:
            client_headers[key] = val
    client_headers["x-nexo-endpoint"] = token

    try:
        async with websockets.connect(
            upstream_url,
            additional_headers=client_headers,  # type: ignore[arg-type]
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as upstream:
            async def client_to_upstream() -> None:
                try:
                    while True:
                        msg = await ws.receive()
                        if msg["type"] == "websocket.disconnect":
                            return
                        data = msg.get("bytes")
                        if data is not None:
                            await upstream.send(data)
                        else:
                            text = msg.get("text")
                            if text is not None:
                                await upstream.send(text)
                except (WebSocketDisconnect, Exception):
                    return

            async def upstream_to_client() -> None:
                try:
                    async for message in upstream:
                        if isinstance(message, (bytes, bytearray)):
                            await ws.send_bytes(bytes(message))
                        else:
                            await ws.send_text(message)
                except Exception:
                    return

            done, pending = await asyncio.wait(
                {
                    asyncio.create_task(client_to_upstream()),
                    asyncio.create_task(upstream_to_client()),
                },
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
    except (websockets.exceptions.WebSocketException, OSError) as exc:
        log.info("gateway ws upstream failed: %s", type(exc).__name__)
        await ws.close(code=1014, reason="upstream unavailable")
        return
    finally:
        try:
            await ws.close()
        except Exception:
            pass
