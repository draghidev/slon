using System.Diagnostics;

namespace Slon.Pg.Serialization;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public enum DataFormat : byte
{
    Binary,
    Text
}

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public static class DataFormatExtensions
{
    public static DataFormat ToDataFormat(this PgFormat format) => format switch
    {
        PgFormat.Binary => DataFormat.Binary,
        PgFormat.Text => DataFormat.Text,
        _ => throw new UnreachableException()
    };

    public static PgFormat ToPgFormat(this DataFormat format) => format switch
    {
        DataFormat.Binary => PgFormat.Binary,
        DataFormat.Text => PgFormat.Text,
        _ => throw new UnreachableException()
    };
}
