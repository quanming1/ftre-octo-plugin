# ftre Octo Plugin

Octo IM channel plugin for ftre.

It bridges Octo's WuKongIM binary WebSocket protocol into ftre's Python channel
runtime:

```text
Octo Server <-> octo-bridge.js <-> OctoChannel.py <-> ftre EventBus <-> AgentLoop
```

## Files

- `octo_channel.py` - ftre external plugin and `OctoChannel` implementation.
- `octo-bridge.js` - Node.js WuKongIM bridge.
- `package.json` - Node runtime dependencies for the bridge.

The current ftre plugin loader scans only `~/.ftre/plugins/*.py`. Keep the
compatibility shim at `~/.ftre/plugins/octo_channel.py`; the real project lives
in `~/.ftre/plugins/octo-plugin`.

## Install

```powershell
cd $env:USERPROFILE\.ftre\plugins\octo-plugin
npm install
```

Configure `~/.ftre/config.json`:

```json
{
  "plugins": [
    {
      "name": "octo_channel",
      "enabled": true,
      "config": {
        "bot_token": "bf_xxx",
        "api_url": "https://im.deepminer.com.cn/api",
        "bridge_port": 9876
      }
    }
  ]
}
```

## Runtime Notes

- The plugin calls `POST /v1/bot/register` to get `robot_id`, then filters
  non-event messages from that same `robot_id` to prevent bot self-echo loops.
- Octo conversations are mapped into ftre sessions through
  `SessionManager.get_or_create_external_session()`.
- The bridge is launched from this project directory, so Node dependencies are
  resolved from `octo-plugin/node_modules`.

## Checks

```powershell
node --check .\octo-bridge.js
py -m py_compile .\octo_channel.py
```

From the ftre repository:

```powershell
$env:PYTHONPATH="$env:USERPROFILE\.ftre\plugins"
py -m pytest tests\test_octo_channel.py tests\test_external_sessions.py -q
```
