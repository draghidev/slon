using System.Data.Common;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbColumn" />
public sealed class SlonDbColumn : DbColumn
{
    /// <summary>
    /// The <see cref="Slon.SlonDbType" /> of the column.
    /// </summary>
    public SlonDbType SlonDbType { get; private set; }
}
