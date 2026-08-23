using System.Data.Common;
using Slon.Runtime.CompilerServices;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbBatchCommandCollection" />
public sealed class SlonBatchCommands : DbBatchCommandCollection, IList<SlonBatchCommand>
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
        ArgumentNullException.ThrowIfNull(item);
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
    public override bool Contains(DbBatchCommand item) => item is SlonBatchCommand command && Contains(command);

    /// <inheritdoc/>
    public void CopyTo(SlonBatchCommand[] array, int arrayIndex) => CopyTo((DbBatchCommand[])array, arrayIndex);

    /// <inheritdoc/>
    public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        if ((uint)arrayIndex > array.Length)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));
        if (Count > array.Length - arrayIndex)
            throw new ArgumentException("Destination array is not long enough.", nameof(array));

        for (var i = 0; i < Count; i++)
            array[arrayIndex + i] = List[i];
    }

    /// <inheritdoc/>
    public int IndexOf(SlonBatchCommand item) => List.IndexOf(item);

    /// <inheritdoc/>
    public override int IndexOf(DbBatchCommand item) => item is SlonBatchCommand command ? IndexOf(command) : -1;

    /// <inheritdoc/>
    public void Insert(int index, SlonBatchCommand item)
    {
        ArgumentNullException.ThrowIfNull(item);
        ThrowIfReadOnly();
        List.Insert(index, item);
    }

    /// <inheritdoc/>
    public override void Insert(int index, DbBatchCommand item) => Insert(index, Cast(item));

    /// <inheritdoc/>
    public bool Remove(SlonBatchCommand item)
    {
        ThrowIfReadOnly();
        return List.Remove(item);
    }

    /// <inheritdoc/>
    public override bool Remove(DbBatchCommand item)
    {
        ThrowIfReadOnly();
        return item is SlonBatchCommand command && List.Remove(command);
    }

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
        ArgumentNullException.ThrowIfNull(command);
        ThrowIfReadOnly();
        if ((uint)index >= Count)
            throw new ArgumentOutOfRangeException(nameof(index));
        List.AsSpan()[index] = command;
    }

    /// <inheritdoc/>
    protected override void SetBatchCommand(int index, DbBatchCommand command)
        => SetBatchCommand(index, Cast(command));

    static SlonBatchCommand Cast(DbBatchCommand? value)
    {
        ArgumentNullException.ThrowIfNull(value);
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
