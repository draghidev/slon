using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace Slon;

public sealed partial class SlonDataReader
{
    async ValueTask<DataTable?> GetSchemaTableCore(CancellationToken cancellationToken = default)
    {
        if (FieldCountCore == 0) // No resultset
            return null;

        var table = new DataTable("SchemaTable");

        // Important to match SqlClient's column order, certain ADO.NET libraries naively assume identical ordering.
        // See: https://github.com/npgsql/npgsql/issues/1671
        table.Columns.Add("ColumnName", typeof(string));
        table.Columns.Add("ColumnOrdinal", typeof(int));
        table.Columns.Add("ColumnSize", typeof(int));
        table.Columns.Add("NumericPrecision", typeof(int));
        table.Columns.Add("NumericScale", typeof(int));
        table.Columns.Add("IsUnique", typeof(bool));
        table.Columns.Add("IsKey", typeof(bool));
        table.Columns.Add("BaseServerName", typeof(string));
        table.Columns.Add("BaseCatalogName", typeof(string));
        table.Columns.Add("BaseColumnName", typeof(string));
        table.Columns.Add("BaseSchemaName", typeof(string));
        table.Columns.Add("BaseTableName", typeof(string));
        table.Columns.Add("DataType", typeof(Type));
        table.Columns.Add("AllowDBNull", typeof(bool));
        table.Columns.Add("ProviderType", typeof(SlonDbType));
        table.Columns.Add("IsAliased", typeof(bool));
        table.Columns.Add("IsExpression", typeof(bool));
        table.Columns.Add("IsIdentity", typeof(bool));
        table.Columns.Add("IsAutoIncrement", typeof(bool));
        table.Columns.Add("IsRowVersion", typeof(bool));
        table.Columns.Add("IsHidden", typeof(bool));
        table.Columns.Add("IsLong", typeof(bool));
        table.Columns.Add("IsReadOnly", typeof(bool));
        table.Columns.Add("ProviderSpecificDataType", typeof(Type));
        table.Columns.Add("DataTypeName", typeof(string));

        foreach (var column in await GetColumnSchemaCore<SlonDbColumn>(cancellationToken)
                     .ConfigureAwait(false))
        {
            var row = table.NewRow();

            row["ColumnName"] = column.ColumnName;
            row["ColumnOrdinal"] = column.ColumnOrdinal ?? -1;
            row["ColumnSize"] = column.ColumnSize ?? -1;
            row["NumericPrecision"] = column.NumericPrecision ?? 0;
            row["NumericScale"] = column.NumericScale ?? 0;
            row["IsUnique"] = column.IsUnique == true;
            row["IsKey"] = column.IsKey == true;
            row["BaseServerName"] = "";
            row["BaseCatalogName"] = column.BaseCatalogName;
            row["BaseColumnName"] = column.BaseColumnName;
            row["BaseSchemaName"] = column.BaseSchemaName;
            row["BaseTableName"] = column.BaseTableName;
            row["DataType"] = column.DataType;
            row["AllowDBNull"] = (object?)column.AllowDBNull ?? DBNull.Value;
            row["ProviderType"] = column.SlonDbType;
            row["IsAliased"] = column.IsAliased == true;
            row["IsExpression"] = column.IsExpression == true;
            row["IsIdentity"] = column.IsIdentity == true;
            row["IsAutoIncrement"] = column.IsAutoIncrement == true;
            row["IsRowVersion"] = false;
            row["IsHidden"] = column.IsHidden == true;
            row["IsLong"] = column.IsLong == true;
            row["IsReadOnly"] = column.IsReadOnly == true;
            row["ProviderSpecificDataType"] = column.DataType;
            row["DataTypeName"] = column.DataTypeName;

            table.Rows.Add(row);
        }

        return table;
    }

    ValueTask<ReadOnlyCollection<TColumn>> GetColumnSchemaCore<TColumn>(
        CancellationToken cancellationToken = default)
        where TColumn : DbColumn
    {
        Debug.Assert(typeof(TColumn) == typeof(DbColumn) || typeof(TColumn) == typeof(SlonDbColumn));
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<ReadOnlyCollection<TColumn>>(cancellationToken);

        if (Current is null || FieldCountCore is 0)
            return ValueTask.FromResult(new ReadOnlyCollection<TColumn>([]));

        var description = FieldReader.RowDescription;
        var columns = new TColumn[description.FieldCount];
        for (var i = 0; i < columns.Length; i++)
        {
            ref readonly var field = ref description[i];
            columns[i] = (TColumn)(DbColumn)new SlonDbColumn(field.Name, i,
                FieldReader.GetFieldType(i), FieldReader.GetDataTypeName(i),
                FieldReader.GetSlonDbType(i));
        }
        return ValueTask.FromResult(new ReadOnlyCollection<TColumn>(columns));
    }

    /// <inheritdoc/>
    public override DataTable? GetSchemaTable()
    {
        ThrowIfClosedOrDisposed();
        var task = GetSchemaTableCore();
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <inheritdoc/>
    public override Task<DataTable?> GetSchemaTableAsync(
        CancellationToken cancellationToken = default)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<DataTable?>(exception);

        return GetSchemaTableCore(cancellationToken).AsTask();
    }

    /// <summary>Gets the column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</returns>
    ReadOnlyCollection<DbColumn> IDbColumnSchemaGenerator.GetColumnSchema()
    {
        ThrowIfClosedOrDisposed();
        var task = GetColumnSchemaCore<DbColumn>();
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <summary>Gets the column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:Slon.SlonDbColumn" /> collection).</returns>
    public ReadOnlyCollection<SlonDbColumn> GetColumnSchema()
    {
        ThrowIfClosedOrDisposed();
        var task = GetColumnSchemaCore<SlonDbColumn>();
        Debug.Assert(task.IsCompleted);
        return task.Result;
    }

    /// <summary>Gets the column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</summary>
    /// <returns>The column schema (<see cref="T:System.Data.Common.DbColumn" /> collection).</returns>
    public override Task<ReadOnlyCollection<DbColumn>> GetColumnSchemaAsync(
        CancellationToken cancellationToken = default)
    {
        if (GetExceptionIfClosedOrDisposed() is { } exception)
            return Task.FromException<ReadOnlyCollection<DbColumn>>(exception);

        return GetColumnSchemaCore<DbColumn>(cancellationToken).AsTask();
    }
}
