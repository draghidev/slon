using System.Data.Common;
using Slon.Runtime.CompilerServices;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbBatchCommandCollection" />
public sealed class SlonBatchCommands: DbBatchCommandCollection, IList<SlonBatchCommand>
{
    readonly FieldRef<AdoBatchCore<SlonBatchCommand>> _batchRef;

    internal SlonBatchCommands(FieldRef<AdoBatchCore<SlonBatchCommand>> batchRef) => _batchRef = batchRef;

    ref AdoCommandList<SlonBatchCommand> List => ref _batchRef.Invoke().Commands;

    /// <inheritdoc/>
    public override int Count => List.Count;

    /// <inheritdoc/>
    public override bool IsReadOnly => _batchRef.Invoke().IsReadOnly;

    /// <inheritdoc/>
    IEnumerator<SlonBatchCommand> IEnumerable<SlonBatchCommand>.GetEnumerator()
    {
        for (var i = 0; i < List.Count; i++)
            yield return List[i];
    }

    /// <inheritdoc/>
    public override IEnumerator<DbBatchCommand> GetEnumerator() => ((IEnumerable<SlonBatchCommand>)this).GetEnumerator();

    /// <inheritdoc/>
    public void Add(SlonBatchCommand item)
    {
        ThrowIfReadOnly();
        List.Add(item);
    }

    /// <inheritdoc/>
    public override void Add(DbBatchCommand item) => Add(Cast(item));

    /// <inheritdoc/>
    public override void Clear()
    {
        ThrowIfReadOnly();
        List.Clear();
    }

    /// <inheritdoc/>
    public bool Contains(SlonBatchCommand item) => List.Contains(item);

    /// <inheritdoc/>
    public override bool Contains(DbBatchCommand item) => Contains(Cast(item));

    /// <inheritdoc/>
    public void CopyTo(SlonBatchCommand[] array, int arrayIndex) => List.CopyTo(array, arrayIndex);

    /// <inheritdoc/>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        if (array is not SlonBatchCommand[] slonArray)
            throw new InvalidCastException(
                $"{nameof(array)} is not of type {nameof(SlonBatchCommand)} and cannot be used in this batch command collection.");

        CopyTo(slonArray, arrayIndex);
    }

    /// <inheritdoc/>
    public int IndexOf(SlonBatchCommand item) => List.IndexOf(item);

    /// <inheritdoc/>
    public override int IndexOf(DbBatchCommand item) => IndexOf(Cast(item));

    /// <inheritdoc/>
    public void Insert(int index, SlonBatchCommand item) => SetBatchCommand(index, item);

    /// <inheritdoc/>
    public override void Insert(int index, DbBatchCommand item) => Insert(index, Cast(item));

    /// <inheritdoc/>
    public bool Remove(SlonBatchCommand item)
    {
        ThrowIfReadOnly();
        return List.Remove(item);
    }

    /// <inheritdoc/>
    public override bool Remove(DbBatchCommand item) => Remove(Cast(item));

    /// <inheritdoc/>
    public override void RemoveAt(int index)
    {
        ThrowIfReadOnly();
        List.RemoveAt(index);
    }

    /// <inheritdoc cref="IList{T}.this" />
    public new SlonBatchCommand this[int index]
    {
        get => List[index];
        set => SetBatchCommand(index, value);
    }

    /// <inheritdoc/>
    protected override DbBatchCommand GetBatchCommand(int index)
        => List[index];

    void SetBatchCommand(int index, SlonBatchCommand command)
    {
        ThrowIfReadOnly();
        List.AsSpan()[index] = command;
    }

    /// <inheritdoc/>
    protected override void SetBatchCommand(int index, DbBatchCommand command)
        => SetBatchCommand(index, Cast(command));

    static SlonBatchCommand Cast(DbBatchCommand? value)
    {
        return value as SlonBatchCommand ?? ThrowInvalidCastException(value);

        static SlonBatchCommand ThrowInvalidCastException(DbBatchCommand? value) =>
            throw new InvalidCastException(
                $"The value \"{value}\" is not of type \"{nameof(SlonBatchCommand)}\" and cannot be used in this batch command collection.");
    }

    void ThrowIfReadOnly()
    {
        _batchRef.Invoke().ThrowIfDisposedOrReadOnly();
    }
}
