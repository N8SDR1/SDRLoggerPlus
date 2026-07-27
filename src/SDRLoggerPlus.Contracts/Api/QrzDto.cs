namespace SDRLoggerPlus.Contracts.Api;

public record QrzSettingsRequest(
    string Username,
    string Password,
    string? ApiKey = null,
    bool Enabled = true
);

public record QrzSubscriptionResponse(
    bool IsValid,
    bool HasXmlSubscription,
    string? Username,
    string? Message,
    DateTime? ExpirationDate
);

public record QrzUploadRequest(
    IEnumerable<string> QsoIds
);

public record QrzUploadResponse(
    int TotalCount,
    int SuccessCount,
    int FailedCount,
    IEnumerable<QrzUploadResultDto> Results
);

public record QrzUploadResultDto(
    bool Success,
    string? LogId,
    string? Message,
    string? QsoId
);

public record QrzCallsignResponse(
    string Callsign,
    string? Name,
    string? FirstName,
    string? Address,
    string? City,
    string? State,
    string? Country,
    string? Grid,
    double? Latitude,
    double? Longitude,
    int? Dxcc,
    int? CqZone,
    int? ItuZone,
    string? Email,
    string? QslManager,
    string? ImageUrl,
    DateTime? LicenseExpiration,
    // "Will accept" flags from the QRZ callbook profile. Three-state:
    // true / false / null-for-unknown — null is NOT a refusal.
    bool? Lotw = null,
    bool? Eqsl = null,
    bool? Mqsl = null
);
