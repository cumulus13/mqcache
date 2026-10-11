# Command line client

```
mqcache-cli [options] [command [args...]]
  -e, --endpoint <spec>   . | host | os:host | tcp:10.0.0.5     (default ".")
  -q, --queue <name>      request queue name                    (default "mqcache")
  -a, --auth <password>
  -t, --timeout <ms>      per attempt (default 5000)    --retries <n> (default 2)
      --priority <p>      lowest .. highest             --no-tx   server queue is non-transactional
      --raw               raw output (default when stdout is redirected)
```

Interactive shell:

```
local/mqcache> SET greeting "hello world" EX 60
OK
local/mqcache> GET greeting
"hello world"
local/mqcache> HGETALL session:9
1) "ip"
2) "10.0.0.1"
local/mqcache> SUBSCRIBE news          (Ctrl+C returns to the prompt)
local/mqcache> BENCHMARK 5000 8        (example output; numbers depend entirely on your hardware and MSMQ setup)
5000 SET+GET pairs, 8 threads, 3.1s => 3225 ops/s, errors=0
```

One-shot (scripting): `mqcache-cli -e tcp:10.0.0.5 -a secret GET greeting`. Exit code is `0` on success, `1` on error replies or failures.

Quoting: both `"double"` (with `\n \r \t \"` escapes) and `'single'` quotes are accepted.
