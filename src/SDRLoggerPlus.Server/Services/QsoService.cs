using Microsoft.AspNetCore.SignalR;
using MongoDB.Bson;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

public class QsoService : IQsoService
{
    private readonly IQsoRepository _repository;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly ISpotStatusService? _spotStatusService;
    private readonly ClubLogService? _clubLog;
    private readonly HrdLogService? _hrdLog;

    public QsoService(IQsoRepository repository, IHubContext<LogHub, ILogHubClient> hub,
        ISpotStatusService? spotStatusService = null, ClubLogService? clubLog = null,
        HrdLogService? hrdLog = null)
    {
        _repository = repository;
        _hub = hub;
        _spotStatusService = spotStatusService;
        _clubLog = clubLog;
        _hrdLog = hrdLog;
    }

    public async Task<QsoResponse?> GetByIdAsync(string id)
    {
        var qso = await _repository.GetByIdAsync(id);
        return qso is null ? null : MapToResponse(qso);
    }

    public async Task<PaginatedQsoResponse> GetQsosAsync(QsoSearchRequest request)
    {
        var (items, totalCount) = await _repository.SearchAsync(request);
        var totalPages = (int)Math.Ceiling(totalCount / (double)request.Limit);
        var page = (request.Skip / request.Limit) + 1;

        return new PaginatedQsoResponse(
            Items: items.Select(MapToResponse),
            TotalCount: totalCount,
            Page: page,
            PageSize: request.Limit,
            TotalPages: totalPages
        );
    }

    public async Task<QsoResponse> CreateAsync(CreateQsoRequest request)
    {
        var qso = new Qso
        {
            Callsign = request.Callsign.ToUpperInvariant(),
            QsoDate = request.QsoDate,
            TimeOn = request.TimeOn,
            Band = request.Band,
            Mode = request.Mode,
            Frequency = request.Frequency,
            RstSent = request.RstSent,
            RstRcvd = request.RstRcvd,
            Comment = request.Comment,
            // v1.x General-mode fields — Contest maps into ContestInfo.ContestId
            // (the free-text contest/event/park identifier the operator
            // types), Notes stays as a top-level free-text string, and Qth
            // sits on the nested StationInfo (worked-station QTH).
            Contest = !string.IsNullOrWhiteSpace(request.Contest)
                ? new ContestInfo { ContestId = request.Contest }
                : null,
            Notes = request.Notes,
            Station = new StationInfo
            {
                Name = request.Name,
                Qth = request.Qth,
                Grid = request.Grid,
                Country = request.Country
            }
        };

        // POTA tagging — write the standard ADIF POTA fields into
        // AdifExtra so PotaStatistics picks them up and QSOs round-
        // trip through ADIF export cleanly.
        //   my_pota_ref = park the operator is activating
        //   pota_ref    = worked-station's park for P2P contacts
        if (!string.IsNullOrWhiteSpace(request.MyPotaRef) || !string.IsNullOrWhiteSpace(request.PotaRef))
        {
            qso.AdifExtra ??= new BsonDocument();
            if (!string.IsNullOrWhiteSpace(request.MyPotaRef))
                qso.AdifExtra["my_pota_ref"] = request.MyPotaRef.Trim().ToUpperInvariant();
            if (!string.IsNullOrWhiteSpace(request.PotaRef))
                qso.AdifExtra["pota_ref"] = request.PotaRef.Trim().ToUpperInvariant();
        }

        var created = await _repository.CreateAsync(qso);

        // Fire-and-forget Club Log realtime upload — logging must never block
        // on (or fail because of) a slow external service. The service itself
        // no-ops when disabled and self-blocks on auth failure.
        if (_clubLog != null)
        {
            _ = Task.Run(() => _clubLog.UploadQsoAsync(created));
        }

        // Fire-and-forget HRDLog.net realtime upload (same rationale as above —
        // the service no-ops when disabled).
        if (_hrdLog != null)
        {
            _ = Task.Run(() => _hrdLog.UploadQsoAsync(created));
        }

        // Update spot status cache incrementally
        _spotStatusService?.OnQsoLogged(
            created.Callsign,
            created.Country,
            created.Band,
            created.Mode);

        // Broadcast to all clients via SignalR
        await _hub.BroadcastQso(new QsoLoggedEvent(
            created.Id,
            created.Callsign,
            created.QsoDate,
            created.TimeOn,
            created.Band,
            created.Mode,
            created.Frequency,
            created.RstSent,
            created.RstRcvd,
            created.Station?.Grid
        ));

        return MapToResponse(created);
    }

    public async Task<QsoResponse?> UpdateAsync(string id, UpdateQsoRequest request)
    {
        var existing = await _repository.GetByIdAsync(id);
        if (existing is null) return null;

        if (request.Callsign != null) existing.Callsign = request.Callsign.ToUpperInvariant();
        if (request.QsoDate.HasValue) existing.QsoDate = request.QsoDate.Value;
        if (request.TimeOn != null) existing.TimeOn = request.TimeOn;
        if (request.Band != null) existing.Band = request.Band;
        if (request.Mode != null) existing.Mode = request.Mode;
        if (request.Frequency.HasValue) existing.Frequency = request.Frequency;
        if (request.RstSent != null) existing.RstSent = request.RstSent;
        if (request.RstRcvd != null) existing.RstRcvd = request.RstRcvd;
        if (request.Comment != null) existing.Comment = request.Comment;

        existing.Station ??= new StationInfo();
        if (request.Name != null) existing.Station.Name = request.Name;
        if (request.Grid != null) existing.Station.Grid = request.Grid;
        if (request.Country != null) existing.Station.Country = request.Country;

        await _repository.UpdateAsync(id, existing);
        return MapToResponse(existing);
    }

    public async Task<bool> DeleteAsync(string id)
    {
        return await _repository.DeleteAsync(id);
    }

    public async Task<QsoStatistics> GetStatisticsAsync()
    {
        return await _repository.GetStatisticsAsync();
    }

    private static QsoResponse MapToResponse(Qso qso) => new(
        qso.Id,
        qso.Callsign,
        qso.QsoDate,
        qso.TimeOn,
        qso.TimeOff,
        qso.Band,
        qso.Mode,
        qso.Frequency,
        qso.RstSent,
        qso.RstRcvd,
        new StationInfoDto(
            qso.Name ?? qso.Station?.Name,
            qso.Grid ?? qso.Station?.Grid,
            qso.Country ?? qso.Station?.Country,
            qso.Dxcc ?? qso.Station?.Dxcc,
            qso.Station?.State,
            qso.Continent ?? qso.Station?.Continent,
            qso.Station?.Latitude,
            qso.Station?.Longitude
        ),
        qso.Comment,
        qso.CreatedAt
    );
}
