// File: sdk/tools/GoldenVectors/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Writes wire-protocol golden vectors (hex) from the reference .NET codec; every SDK must reproduce them byte for byte.
// License: MIT

using System.Text;
using System.Text.Json;
using MQCache.Core.Protocol;

string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();
string H(string s) => Hex(Encoding.UTF8.GetBytes(s));
var ids = new[] { "11111111-2222-3333-4444-555555555555", "00000000-0000-0000-0000-000000000000", "a1b2c3d4-e5f6-4718-8293-a4b5c6d7e8f9" };

var requests = new List<object>();
void Req(string name, string id, string? auth, bool atomic, bool noReply, params string[][] cmds) // args as hex
{
    var commands = cmds.Select(c => new Command(Encoding.UTF8.GetString(Convert.FromHexString(c[0])), c.Skip(1).Select(Convert.FromHexString).ToArray())).ToList();
    var bytes = WireCodec.EncodeRequest(new RequestEnvelope(Guid.Parse(id), auth, atomic, noReply, commands));
    requests.Add(new { name, id, auth, atomic, no_reply = noReply, commands = cmds, hex = Hex(bytes) });
}
Req("ping", ids[0], null, false, false, new[] { H("PING") });
Req("set_get_auth", ids[2], "s3cr\u00e9t", true, false, new[] { H("SET"), H("k\u00e9y"), H("v\u00e4lue"), H("EX"), H("60") }, new[] { H("GET"), H("k\u00e9y") });
Req("binary", ids[1], null, false, true, new[] { H("SET"), H("bin"), "00ff01fe80" });
Req("empty_arg", ids[0], null, false, false, new[] { H("SET"), H("k"), "" });

var responses = new List<object>();
void Resp(string name, string id, RespValue[] values, object spec)
{
    var bytes = WireCodec.EncodeResponse(new ResponseEnvelope(Guid.Parse(id), values));
    responses.Add(new { name, id, spec, hex = Hex(bytes) });
}
Resp("ok_pong", ids[0], new[] { RespValue.Ok, RespValue.Pong }, new object[] { new object[] { "status", "OK" }, new object[] { "status", "PONG" } });
Resp("scalars", ids[2], new[] { RespValue.Int(-42), RespValue.Int(long.MaxValue), RespValue.Nil, RespValue.Error("WRONGTYPE bad"), RespValue.Bulk("h\u00e9llo") },
    new object[] { new object[] { "int", "-42" }, new object[] { "int", long.MaxValue.ToString() }, new object[] { "null" }, new object[] { "error", "WRONGTYPE bad" }, new object[] { "bulk", H("h\u00e9llo") } });
Resp("nested", ids[1], new[] { RespValue.Arr(new[] { RespValue.Int(1), RespValue.Arr(new[] { RespValue.Bulk("n"), RespValue.Nil }), RespValue.EmptyArray }) },
    new object[] { new object[] { "array", new object[] { new object[] { "int", "1" }, new object[] { "array", new object[] { new object[] { "bulk", H("n") }, new object[] { "null" } } }, new object[] { "array", new object[0] } } } });
Resp("binary_bulk", ids[2], new[] { RespValue.Bulk(new byte[] { 0, 255, 1, 254 }) }, new object[] { new object[] { "bulk", "00ff01fe" } });

var pubs = new List<object>();
foreach (var (ch, pat, payload) in new[] { ("news", (string?)null, new byte[] { 104, 105 }), ("news.sport", "news.*", new byte[] { 0, 1, 2 }) })
    pubs.Add(new { channel = ch, pattern = pat, payload = Hex(payload), hex = Hex(PubSubCodec.Encode(new PubMessage(ch, pat, payload))) });

File.WriteAllText(args.Length > 0 ? args[0] : "sdk/testdata/golden.json", JsonSerializer.Serialize(new { requests, responses, pubs }, new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"{requests.Count} requests, {responses.Count} responses, {pubs.Count} pubs");
