using Xunit;

namespace SDRLoggerPlus.Server.Tests.Fixtures;

/// <summary>
/// Serialises test classes that map POCOs through a raw
/// <c>new LiteDatabase(path)</c>, which uses the process-wide
/// <c>BsonMapper.Global</c>.
///
/// That mapper builds its per-type member list lazily on first use and is not
/// safe for concurrent first access — two xUnit classes serialising
/// UserSettings at the same moment throw "Collection was modified" from inside
/// BsonMapper. It is the same hazard LiteDbContext's constructor pre-warms its
/// own mapper against.
///
/// This is a test-harness constraint, not a product one: the app has a single
/// context with a single pre-warmed mapper. Marking the affected classes into
/// one collection keeps them off each other rather than papering over a
/// product race that does not exist.
/// </summary>
[CollectionDefinition("LiteDbMapper", DisableParallelization = true)]
public class LiteDbMapperCollection
{
}
