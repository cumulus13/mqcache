// File: src/MQCache.Core/Log.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-11
// Description: Minimal logging abstraction so Core/Server stay independent of any logging framework.
// License: MIT

namespace MQCache.Core;

public enum LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 }

public interface ILog
{
    bool IsEnabled(LogLevel level);
    void Write(LogLevel level, string message, Exception? exception = null);
}

public static class LogExtensions
{
    public static void Debug(this ILog log, string message) => log.Write(LogLevel.Debug, message);
    public static void Info(this ILog log, string message) => log.Write(LogLevel.Info, message);
    public static void Warn(this ILog log, string message, Exception? ex = null) => log.Write(LogLevel.Warn, message, ex);
    public static void Error(this ILog log, string message, Exception? ex = null) => log.Write(LogLevel.Error, message, ex);
}

public sealed class NullLog : ILog
{
    public static readonly NullLog Instance = new();
    public bool IsEnabled(LogLevel level) => false;
    public void Write(LogLevel level, string message, Exception? exception = null) { }
}
