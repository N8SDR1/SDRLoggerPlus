namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// The AI provider answered, but not with something usable (bad key, retired
/// model, quota, empty reply…). The message is written for the operator and is
/// safe to show as-is.
/// </summary>
public sealed class AiProviderException(string message) : Exception(message);
