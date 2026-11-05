# Per-account Claude usage buffers

Set **Usage buffer (%)** in a Claude credential's settings in the companion
[management UI fork](https://github.com/Ben2W/Cli-Proxy-API-Management-Center).
A value of `10` reserves approximately 10%: the proxy excludes that credential
when either its five-hour or shared weekly subscription usage reaches 90%.
Set `0` to disable the reserve. Other credentials retain their existing behavior.

The reserve is stored as `usage_buffer_percent` in the credential's auth JSON.
It is configurable with `PATCH /v8/management/credentials/fields`:

```json
{"name":"claude-example.json","usage_buffer_percent":10}
```

Only Claude credentials support this setting. Whole percentages from 0 to 99
are accepted. The v8 credential list advertises `usage_buffer_supported`,
`usage_buffer_percent`, and a non-secret `usage_buffer` status with `percent`,
`paused`, `reason`, `checked_at`, `five_hour`, and `seven_day` observations.

The monitor runs inside the proxy and uses the credential's provider executor
for authenticated, proxy-aware usage queries. It checks each configured account
every minute. Missing, failed, or older-than-three-minute observations pause
routing until a fresh measurement succeeds. Fresh measurements below the
threshold restore routing automatically after a window resets. The ordinary
manual disabled flag is independent and is never changed by the monitor.

The reserve is approximate: usage reporting delay, already running requests,
and direct use outside this proxy can consume some of it. No new generations
are sent just to measure usage. The monitor stops with the service context.

For a locally built management UI, copy `dist/index.html` to the proxy's
`static/management.html`. Set `management.disable-auto-update-panel: true` to
keep upstream UI releases from replacing that build. No management keys or
provider credentials belong in either repository.
