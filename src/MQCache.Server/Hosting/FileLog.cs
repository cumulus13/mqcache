// File: src/MQCache.Server/Hosting/FileLog.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Thread-safe console + daily-rolling file logger implementing ILog.
// License: MIT

using System.Globalization;
using MQCache.Core;

namespace MQCache.Server.Hosting;

public sealed class FileLog : ILog, IDisposable
{
    private readonly LogLevel _min;
    private readonly string? _dir;
    private readonly string _prefix;
    private readonly bool _console;
    private readonly object _gate = new();
    private string _currentDay = "";
    private StreamWriter? _writer;

    public FileLog(LogLevel min, string? logFile, bool console)
    {
        _min = min; _console = console;
        if (string.IsNullOrWhiteSpace(logFile)) return;
        var full = Path.GetFullPath(logFile);
        _dir = Path.GetDirectoryName(full);
        _prefix = Path.GetFileNameWithoutExtension(full);
        if (_dir is not null) Directory.CreateDirectory(_dir);
    }

    public bool IsEnabled(LogLevel level) => level >= _min;

    public void Write(LogLevel level, string message, Exception? exception = null)
    {
        if (!IsEnabled(level)) return;
        var now = DateTime.Now;
        var line = $"{now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)} [{level.ToString().ToUpperInvariant(),-5}] {message}" +
                   (exception is null ? "" : Environment.NewLine + exception);
        lock (_gate)
        {
            if (_console) Console.WriteLine(line);
            if (_dir is null) return;
            try
            {
                var day = now.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
                if (day != _currentDay)
                {
                    _writer?.Dispose();
                    _writer = new StreamWriter(new FileStream(Path.Combine(_dir, $"{_prefix}-{day}.log"), FileMode.Append, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
                    _currentDay = day;
                }
                _writer!.WriteLine(line);
            }
            catch (IOException) { /* never let logging take the server down */ }
        }
    }

    public void Dispose() { lock (_gate) _writer?.Dispose(); }
}
