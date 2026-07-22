using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Security;

namespace SDRLoggerPlus.Server.Core.Database.LiteDb;

/// <summary>
/// Settings storage. Credentials are encrypted on the way in and decrypted on
/// the way out, so nothing above this class deals in protected values and
/// nothing below it ever sees plaintext.
///
/// The protector is optional only so existing construction sites (and tests
/// that do not care about credentials) keep working; when it is absent the
/// behaviour is exactly the pre-encryption behaviour.
/// </summary>
public class LiteSettingsRepository : ISettingsRepository
{
    private readonly LiteDbContext _context;
    private readonly ISecretProtector? _protector;

    public LiteSettingsRepository(LiteDbContext context, ISecretProtector? protector = null)
    {
        _context = context;
        _protector = protector;
    }

    public Task<UserSettings?> GetAsync(string id = "default")
    {
        var settings = _context.Settings.FindById(id);
        if (settings != null && _protector != null)
        {
            SettingsSecrets.Transform(settings, _protector.Unprotect);
        }
        return Task.FromResult<UserSettings?>(settings);
    }

    public Task<UserSettings> UpsertAsync(UserSettings settings)
    {
        settings.UpdatedAt = DateTime.UtcNow;

        if (_protector == null)
        {
            _context.Settings.Upsert(settings);
            _context.Database.Checkpoint();
            return Task.FromResult(settings);
        }

        // Encrypt for storage, then restore the caller's object to plaintext.
        // The caller owns this instance and the API echoes it back in the
        // response; handing back ciphertext would put "slp$1$dpapi$..." into
        // the settings form's password boxes and the operator would save it
        // as their new password.
        //
        // The encrypt transform sits INSIDE the try so that a Protect failure
        // partway through the fields still hits the finally — Unprotect passes
        // plaintext through untouched, so restoring a half-protected object is
        // safe and leaves the caller holding exactly what it passed in.
        try
        {
            SettingsSecrets.Transform(settings, _protector.Protect);
            _context.Settings.Upsert(settings);
            _context.Database.Checkpoint();
        }
        finally
        {
            SettingsSecrets.Transform(settings, _protector.Unprotect);
        }

        return Task.FromResult(settings);
    }
}
