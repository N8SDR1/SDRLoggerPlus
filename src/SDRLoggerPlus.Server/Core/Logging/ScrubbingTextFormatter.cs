using System.Globalization;
using Serilog.Events;
using Serilog.Formatting;
using Serilog.Formatting.Display;

namespace SDRLoggerPlus.Server.Core.Logging;

/// <summary>
/// Renders a log event with an inner formatter, then runs the rendered text
/// through <see cref="SecretScrubber"/> before it reaches the sink.
///
/// The per-call-site redaction elsewhere in the codebase handles the leaks we
/// know about. This is the net under it: it applies to every line, including
/// exception text, framework messages, and log statements written after this
/// one, none of which we can review in advance. It cannot recognise a bare
/// credential with no parameter name around it — that is what the call-site
/// redaction is for — so the two are complementary, not redundant.
///
/// The console sink's colour theme is lost by taking over formatting. The
/// console here is not read directly: Electron captures stdout into main.log,
/// where the ANSI codes were never rendered anyway.
/// </summary>
public sealed class ScrubbingTextFormatter : ITextFormatter
{
    public const string DefaultOutputTemplate =
        "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}";

    private readonly ITextFormatter _inner;

    public ScrubbingTextFormatter(ITextFormatter? inner = null)
    {
        _inner = inner ?? new MessageTemplateTextFormatter(DefaultOutputTemplate, CultureInfo.InvariantCulture);
    }

    public void Format(LogEvent logEvent, TextWriter output)
    {
        var rendered = new StringWriter();
        _inner.Format(logEvent, rendered);
        output.Write(SecretScrubber.Redact(rendered.ToString()));
    }
}
