// Vendored from dotnet/runtime ConditionalWeakTable<TKey, TValue> (MIT license, .NET Foundation),
// modified to accept an IEqualityComparer<TKey> so weak-key lifetime is decoupled from key equality.
// Uses public DependentHandle surface (Target / Dependent / TargetAndDependent) since the Unsafe*
// fast paths CWT uses are internal to CoreLib.
// Upstream source:
//   https://github.com/dotnet/runtime/blob/main/src/libraries/System.Private.CoreLib/src/System/Runtime/CompilerServices/ConditionalWeakTable.cs

using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Slon.Runtime.CompilerServices;

sealed class WeakKeyedTable<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    where TKey : class
    where TValue : class?
{
    // Lifetimes of keys and values:
    // Inserting a key and value into the table will not prevent the key from dying, even if the key
    // is strongly reachable from the value. Once the key dies, the table automatically removes the
    // key/value entry on the next resize/sweep.
    //
    // Equality:
    // Keys are matched using the IEqualityComparer<TKey> passed at construction (defaults to
    // EqualityComparer<TKey>.Default). Unlike System.Runtime.CompilerServices.ConditionalWeakTable,
    // this table decouples lifetime (weak key) from identity (custom equality).
    //
    // Thread safety: fully thread-safe. Readers do not take the lock.

    const int InitialCapacity = 8;
    readonly object _lock;
    readonly IEqualityComparer<TKey> _comparer;
    volatile Container _container;
    int _activeEnumeratorRefCount;

    public WeakKeyedTable(IEqualityComparer<TKey>? comparer = null)
    {
        _lock = new object();
        _comparer = comparer ?? EqualityComparer<TKey>.Default;
        _container = new Container(this);
    }

    public IEqualityComparer<TKey> Comparer => _comparer;

    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _container.TryGetValueWorker(key, out value);
    }

    public void Add(TKey key, TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);

        lock (_lock)
        {
            int entryIndex = _container.FindEntry(key, out _);
            if (entryIndex != -1)
                throw new ArgumentException("An item with the same key has already been added.", nameof(key));

            CreateEntry(key, value);
        }
    }

    public bool TryAdd(TKey key, TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);

        lock (_lock)
        {
            int entryIndex = _container.FindEntry(key, out _);
            if (entryIndex != -1)
                return false;

            CreateEntry(key, value);
            return true;
        }
    }

    public void AddOrUpdate(TKey key, TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);

        lock (_lock)
        {
            int entryIndex = _container.FindEntry(key, out _);
            if (entryIndex != -1)
                _container.UpdateValue(entryIndex, value);
            else
                CreateEntry(key, value);
        }
    }

    public bool Remove(TKey key)
    {
        ArgumentNullException.ThrowIfNull(key);

        lock (_lock)
        {
            return _container.Remove(key, out _);
        }
    }

    public bool Remove(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        ArgumentNullException.ThrowIfNull(key);

        lock (_lock)
        {
            return _container.Remove(key, out value);
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            if (_activeEnumeratorRefCount > 0)
                _container.RemoveAllKeys();
            else
                _container = new Container(this);
        }
    }

    public TValue GetOrAdd(TKey key, TValue value)
    {
        if (TryGetValue(key, out TValue? existingValue))
            return existingValue;

        return GetOrAddLocked(key, value);
    }

    public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
    {
        ArgumentNullException.ThrowIfNull(valueFactory);

        if (TryGetValue(key, out TValue? existingValue))
            return existingValue;

        TValue value = valueFactory(key);
        return GetOrAddLocked(key, value);
    }

    public TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
        where TArg : allows ref struct
    {
        ArgumentNullException.ThrowIfNull(valueFactory);

        if (TryGetValue(key, out TValue? existingValue))
            return existingValue;

        TValue value = valueFactory(key, factoryArgument);
        return GetOrAddLocked(key, value);
    }

    TValue GetOrAddLocked(TKey key, TValue value)
    {
        lock (_lock)
        {
            if (_container.TryGetValueWorker(key, out TValue? existingValue))
                return existingValue;

            CreateEntry(key, value);
            return value;
        }
    }

    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
    {
        lock (_lock)
        {
            Container c = _container;
            if (c is null || c.FirstFreeEntry == 0)
                return Enumerable.Empty<KeyValuePair<TKey, TValue>>().GetEnumerator();
            return new Enumerator(this);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();

    sealed class Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
    {
        WeakKeyedTable<TKey, TValue>? _table;
        readonly int _maxIndexInclusive;
        int _currentIndex;
        KeyValuePair<TKey, TValue> _current;

        public Enumerator(WeakKeyedTable<TKey, TValue> table)
        {
            Debug.Assert(table != null);
            Debug.Assert(Monitor.IsEntered(table._lock));
            Debug.Assert(table._container != null);
            Debug.Assert(table._container.FirstFreeEntry > 0);

            _table = table;
            Debug.Assert(table._activeEnumeratorRefCount >= 0);
            table._activeEnumeratorRefCount++;

            _maxIndexInclusive = table._container.FirstFreeEntry - 1;
            _currentIndex = -1;
        }

        ~Enumerator() => Dispose();

        public void Dispose()
        {
            WeakKeyedTable<TKey, TValue>? table = Interlocked.Exchange(ref _table, null);
            if (table != null)
            {
                _current = default;
                lock (table._lock)
                {
                    table._activeEnumeratorRefCount--;
                    Debug.Assert(table._activeEnumeratorRefCount >= 0);
                }
                GC.SuppressFinalize(this);
            }
        }

        public bool MoveNext()
        {
            WeakKeyedTable<TKey, TValue>? table = _table;
            if (table != null)
            {
                lock (table._lock)
                {
                    Container c = table._container;
                    if (c != null)
                    {
                        while (_currentIndex < _maxIndexInclusive)
                        {
                            _currentIndex++;
                            if (c.TryGetEntry(_currentIndex, out TKey? key, out TValue? value))
                            {
                                _current = new KeyValuePair<TKey, TValue>(key, value);
                                return true;
                            }
                        }
                    }
                }
            }

            return false;
        }

        public KeyValuePair<TKey, TValue> Current
        {
            get
            {
                if (_currentIndex < 0)
                    throw new InvalidOperationException("Enumeration has either not started or has already finished.");
                return _current;
            }
        }

        object? IEnumerator.Current => Current;

        public void Reset() { }
    }

    void CreateEntry(TKey key, TValue value)
    {
        Debug.Assert(Monitor.IsEntered(_lock));
        Debug.Assert(key != null);

        Container c = _container;
        if (!c.HasCapacity)
            _container = c = c.Resize();
        c.CreateEntryNoResize(key, value);
    }

    // Entry states:
    //   - Unused (stored at index >= _firstFreeEntry):
    //       depHnd.IsAllocated == false; hashCode/next don't care.
    //   - Used, live key (linked in bucket list):
    //       depHnd.IsAllocated == true; primary != null;
    //       hashCode == comparer.GetHashCode(primary) & int.MaxValue;
    //       next links to next entry in bucket.
    //   - Used, dead key (linked in bucket list):
    //       depHnd.IsAllocated == true; primary == null;
    //       hashCode == <don't care>;
    //       next links to next entry in bucket.
    //   - Removed:
    //       depHnd.IsAllocated == true; primary don't care;
    //       hashCode == -1;
    //       next links to next entry in bucket.
    //
    // Live->dead happens asynchronously via GC. Dead entries get pruned on resize.

    [StructLayout(LayoutKind.Auto)]
    struct Entry
    {
        public DependentHandle depHnd;
        public int HashCode;
        public int Next;
    }

    sealed class Container
    {
        readonly WeakKeyedTable<TKey, TValue> _parent;
        int[] _buckets;
        Entry[] _entries;
        int _firstFreeEntry;
        bool _invalid;
        bool _finalized;
        volatile object? _oldKeepAlive;

        internal Container(WeakKeyedTable<TKey, TValue> parent)
        {
            Debug.Assert(parent != null);
            Debug.Assert(BitOperations.IsPow2(InitialCapacity));

            const int Size = InitialCapacity;
            _buckets = new int[Size];
            for (int i = 0; i < _buckets.Length; i++)
                _buckets[i] = -1;
            _entries = new Entry[Size];

            // Only store the parent after the allocations succeed. Otherwise a partially-constructed
            // Container that gets finalized could clear out the table's live container.
            _parent = parent;
        }

        Container(WeakKeyedTable<TKey, TValue> parent, int[] buckets, Entry[] entries, int firstFreeEntry)
        {
            Debug.Assert(parent != null);
            Debug.Assert(buckets != null);
            Debug.Assert(entries != null);
            Debug.Assert(buckets.Length == entries.Length);
            Debug.Assert(BitOperations.IsPow2(buckets.Length));

            _parent = parent;
            _buckets = buckets;
            _entries = entries;
            _firstFreeEntry = firstFreeEntry;
        }

        internal bool HasCapacity => _firstFreeEntry < _entries.Length;
        internal int FirstFreeEntry => _firstFreeEntry;

        internal void CreateEntryNoResize(TKey key, TValue value)
        {
            Debug.Assert(key != null);
            Debug.Assert(HasCapacity);

            VerifyIntegrity();
            _invalid = true;

            int hashCode = _parent._comparer.GetHashCode(key) & int.MaxValue;
            int newEntry = _firstFreeEntry++;

            _entries[newEntry].HashCode = hashCode;
            _entries[newEntry].depHnd = new DependentHandle(key, value);
            int bucket = hashCode & (_buckets.Length - 1);
            _entries[newEntry].Next = _buckets[bucket];

            Volatile.Write(ref _buckets[bucket], newEntry);

            _invalid = false;
        }

        internal bool TryGetValueWorker(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            Debug.Assert(key != null);

            int entryIndex = FindEntry(key, out object? secondary);
            value = Unsafe.As<TValue>(secondary);
            return entryIndex != -1;
        }

        internal int FindEntry(TKey key, out object? value)
        {
            Debug.Assert(key != null);

            int hashCode = _parent._comparer.GetHashCode(key) & int.MaxValue;
            int bucket = hashCode & (_buckets.Length - 1);
            for (int entriesIndex = Volatile.Read(ref _buckets[bucket]); entriesIndex != -1; entriesIndex = _entries[entriesIndex].Next)
            {
                if (_entries[entriesIndex].HashCode == hashCode)
                {
                    // Bucket-linked entries are always allocated (per entry-state invariants).
                    var (oKey, oValue) = _entries[entriesIndex].depHnd.TargetAndDependent;
                    if (oKey is not null && _parent._comparer.Equals(Unsafe.As<TKey>(oKey), key))
                    {
                        value = oValue;
                        GC.KeepAlive(this);
                        return entriesIndex;
                    }
                }
            }

            GC.KeepAlive(this);
            value = null;
            return -1;
        }

        internal bool TryGetEntry(int index, [NotNullWhen(true)] out TKey? key, [MaybeNullWhen(false)] out TValue value)
        {
            if (index < _entries.Length && _entries[index].depHnd.IsAllocated)
            {
                var (oKey, oValue) = _entries[index].depHnd.TargetAndDependent;
                GC.KeepAlive(this);

                if (oKey != null)
                {
                    key = Unsafe.As<TKey>(oKey);
                    value = Unsafe.As<TValue>(oValue!);
                    return true;
                }
            }

            key = default;
            value = default;
            return false;
        }

        internal void RemoveAllKeys()
        {
            for (int i = 0; i < _firstFreeEntry; i++)
                RemoveIndex(i);
        }

        internal bool Remove(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            VerifyIntegrity();

            int entryIndex = FindEntry(key, out object? valueObject);
            if (entryIndex != -1)
            {
                RemoveIndex(entryIndex);
                value = Unsafe.As<TValue>(valueObject!);
                return true;
            }

            value = null;
            return false;
        }

        void RemoveIndex(int entryIndex)
        {
            Debug.Assert(entryIndex >= 0 && entryIndex < _firstFreeEntry);

            ref Entry entry = ref _entries[entryIndex];

            // Don't free the handle here. Concurrent readers may have already seen the hash code.
            // The handle is freed in Container's finalizer after the table is resized or discarded.
            Volatile.Write(ref entry.HashCode, -1);
            if (entry.depHnd.IsAllocated)
                entry.depHnd.Target = null;
        }

        internal void UpdateValue(int entryIndex, TValue newValue)
        {
            Debug.Assert(entryIndex != -1);

            VerifyIntegrity();
            _invalid = true;

            _entries[entryIndex].depHnd.Dependent = newValue;

            _invalid = false;
        }

        internal Container Resize()
        {
            Debug.Assert(!HasCapacity);

            bool hasExpiredEntries = false;
            int newSize = _buckets.Length;

            if (_parent is null || _parent._activeEnumeratorRefCount == 0)
            {
                for (int entriesIndex = 0; entriesIndex < _entries.Length; entriesIndex++)
                {
                    ref Entry entry = ref _entries[entriesIndex];

                    if (entry.HashCode == -1)
                    {
                        hasExpiredEntries = true;
                        break;
                    }

                    if (entry.depHnd.IsAllocated && entry.depHnd.Target is null)
                    {
                        hasExpiredEntries = true;
                        break;
                    }
                }
            }

            if (!hasExpiredEntries)
                newSize = _buckets.Length * 2;

            return Resize(newSize);
        }

        internal Container Resize(int newSize)
        {
            Debug.Assert(newSize >= _buckets.Length);
            Debug.Assert(BitOperations.IsPow2(newSize));

            int[] newBuckets = new int[newSize];
            for (int bucketIndex = 0; bucketIndex < newBuckets.Length; bucketIndex++)
                newBuckets[bucketIndex] = -1;
            Entry[] newEntries = new Entry[newSize];
            int newEntriesIndex = 0;
            bool activeEnumerators = _parent != null && _parent._activeEnumeratorRefCount > 0;
            bool transferredHandles;

            if (activeEnumerators)
            {
                transferredHandles = true;

                // Active enumerator: preserve indices, just rebuild buckets.
                for (; newEntriesIndex < _entries.Length; newEntriesIndex++)
                {
                    ref Entry oldEntry = ref _entries[newEntriesIndex];
                    ref Entry newEntry = ref newEntries[newEntriesIndex];
                    int hashCode = oldEntry.HashCode;

                    newEntry.HashCode = hashCode;
                    newEntry.depHnd = oldEntry.depHnd;
                    int bucket = hashCode & (newBuckets.Length - 1);
                    newEntry.Next = newBuckets[bucket];
                    newBuckets[bucket] = newEntriesIndex;
                }
            }
            else
            {
                transferredHandles = false;

                for (int entriesIndex = 0; entriesIndex < _entries.Length; entriesIndex++)
                {
                    ref Entry oldEntry = ref _entries[entriesIndex];
                    int hashCode = oldEntry.HashCode;
                    DependentHandle depHnd = oldEntry.depHnd;
                    if (hashCode != -1 && depHnd.IsAllocated)
                    {
                        if (depHnd.Target is not null)
                        {
                            transferredHandles = true;

                            ref Entry newEntry = ref newEntries[newEntriesIndex];

                            newEntry.HashCode = hashCode;
                            newEntry.depHnd = depHnd;
                            int bucket = hashCode & (newBuckets.Length - 1);
                            newEntry.Next = newBuckets[bucket];
                            newBuckets[bucket] = newEntriesIndex;
                            newEntriesIndex++;
                        }
                        else
                        {
                            // Pretend the item was removed so this container's finalizer cleans it up.
                            Volatile.Write(ref oldEntry.HashCode, -1);
                        }
                    }
                }
            }

            var newContainer = new Container(_parent!, newBuckets, newEntries, newEntriesIndex);
            if (activeEnumerators)
            {
                // Old container no longer owns cleanup. Suppress its finalizer.
                GC.SuppressFinalize(this);
            }

            if (transferredHandles)
            {
                // Old container's finalizer will not free transferred handles. New container's
                // finalizer cannot run until this container is no longer in use.
                _oldKeepAlive = newContainer;
            }

            GC.KeepAlive(this);

            return newContainer;
        }

        void VerifyIntegrity()
        {
            if (_invalid)
                throw new InvalidOperationException("Collection was modified during enumeration.");
        }

        ~Container()
        {
            if (_invalid || _parent is null)
                return;

            // The table could have been resurrected. Null out the parent's reference and re-register
            // for finalization. The next pass can free handles without concurrent-use risk.
            if (!_finalized)
            {
                _finalized = true;
                lock (_parent._lock)
                {
                    if (_parent._container == this)
                        _parent._container = null!;
                }
                GC.ReRegisterForFinalize(this);
                return;
            }

            Entry[] entries = _entries;
            _invalid = true;
            _entries = null!;
            _buckets = null!;

            if (entries != null)
            {
                for (int entriesIndex = 0; entriesIndex < entries.Length; entriesIndex++)
                {
                    // Free handles when this container still owns them, or when the entry was explicitly
                    // removed (removed entries are not transferred even if other handles were).
                    if (_oldKeepAlive is null || entries[entriesIndex].HashCode == -1)
                        entries[entriesIndex].depHnd.Dispose();
                }
            }
        }
    }
}
