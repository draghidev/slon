using System.Data.Common;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbColumn" />
public sealed class SlonDbColumn : DbColumn
{
    internal SlonDbColumn(string name, int ordinal, Type dataType, string dataTypeName,
        SlonDbType slonDbType)
    {
        ColumnName = name;
        ColumnOrdinal = ordinal;
        DataType = dataType;
        DataTypeName = dataTypeName;
        SlonDbType = slonDbType;
    }

    /// <summary>
    /// The <see cref="Slon.SlonDbType" /> of the column.
    /// </summary>
    public SlonDbType SlonDbType { get; }
}
