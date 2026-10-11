// File: src/MQCache.Core/Storage/Glob.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Redis-compatible glob matcher (* ? [abc] [a-z] [^x] and backslash escapes).
// License: MIT

namespace MQCache.Core.Storage;

public static class Glob
{
    public static bool IsMatch(string pattern, string text) => Match(pattern, 0, text, 0);

    private static bool Match(string p, int pi, string s, int si)
    {
        while (pi < p.Length)
        {
            switch (p[pi])
            {
                case '*':
                    while (pi + 1 < p.Length && p[pi + 1] == '*') pi++;
                    if (pi + 1 == p.Length) return true;
                    for (var k = si; k <= s.Length; k++)
                        if (Match(p, pi + 1, s, k)) return true;
                    return false;
                case '?':
                    if (si >= s.Length) return false;
                    si++; pi++;
                    break;
                case '[':
                {
                    if (si >= s.Length) return false;
                    pi++;
                    var negate = pi < p.Length && p[pi] == '^';
                    if (negate) pi++;
                    var matched = false;
                    while (pi < p.Length && p[pi] != ']')
                    {
                        if (p[pi] == '\\' && pi + 1 < p.Length) { pi++; if (p[pi] == s[si]) matched = true; pi++; }
                        else if (pi + 2 < p.Length && p[pi + 1] == '-' && p[pi + 2] != ']')
                        {
                            var lo = p[pi]; var hi = p[pi + 2];
                            if (lo > hi) (lo, hi) = (hi, lo);
                            if (s[si] >= lo && s[si] <= hi) matched = true;
                            pi += 3;
                        }
                        else { if (p[pi] == s[si]) matched = true; pi++; }
                    }
                    if (pi < p.Length) pi++; // closing ]
                    if (matched == negate) return false;
                    si++;
                    break;
                }
                case '\\':
                    if (pi + 1 < p.Length) pi++;
                    goto default;
                default:
                    if (si >= s.Length || p[pi] != s[si]) return false;
                    si++; pi++;
                    break;
            }
        }
        return si == s.Length;
    }
}
